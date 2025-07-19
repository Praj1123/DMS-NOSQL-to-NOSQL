import pymongo
import json
import os
import sys
import time
import hashlib
import threading
import concurrent.futures
from datetime import datetime
from pymongo import MongoClient, ReplaceOne, DeleteOne, errors
from bson.objectid import ObjectId
from config.config import *
from .collection_worker import MongoJSONEncoder

# Setup logger
logger = setup_logging("cdc_worker")


def load_checkpoint(collection_name, progress_dir=PROGRESS_DIR):
    path = os.path.join(progress_dir, f"{collection_name}_cdc.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
                return data.get("last_updated_at"), data.get("last_operation_time")
        except json.JSONDecodeError:
            logger.error(
                f"Corrupted CDC checkpoint file for {collection_name}, starting fresh"
            )
            return None, None
    return None, None


def save_checkpoint(
    collection_name,
    last_updated_at,
    last_operation_time=None,
    updates=0,
    deletions=0,
    progress_dir=PROGRESS_DIR,
):
    try:
        path = os.path.join(progress_dir, f"{collection_name}_cdc.json")

        # Load existing data if available to preserve update/deletion counts
        existing_data = {}
        if os.path.exists(path):
            try:
                with open(path) as f:
                    existing_data = json.load(f)
            except Exception:
                pass

        # Update with new data
        checkpoint_data = {
            "last_updated_at": last_updated_at,
            "last_operation_time": last_operation_time,
            "timestamp": datetime.now().isoformat(),
            "updates": existing_data.get("updates", 0) + updates,
            "deletions": existing_data.get("deletions", 0) + deletions,
        }

        # Atomic write by using temporary file
        temp_path = f"{path}.tmp"
        with open(temp_path, "w") as f:
            json.dump(checkpoint_data, f, cls=MongoJSONEncoder)
        os.replace(temp_path, path)  # Atomic replacement
    except Exception as e:
        logger.error(f"Failed to save CDC checkpoint for {collection_name}: {e}")


def log_failed_doc(collection_name, doc, error):
    try:
        fail_log = os.path.join(LOG_DIR, f"{collection_name}_failed_docs.log")
        with open(fail_log, "a") as f:
            f.write(
                json.dumps(
                    {
                        "doc_id": str(doc.get("_id")),
                        "error": str(error),
                        "timestamp": datetime.now().isoformat(),
                        "doc_summary": str(
                            doc.get("_id")
                        ),  # Don't log full doc to save space
                    },
                    cls=MongoJSONEncoder,
                )
                + "\n"
            )
    except Exception as e:
        logger.error(f"Failed to log error for document in {collection_name}: {e}")


def verify_doc(src_collection, tgt_collection, doc_id):
    """Verify a single document matches between source and target"""
    try:
        src_doc = src_collection.find_one({"_id": doc_id})
        tgt_doc = tgt_collection.find_one({"_id": doc_id})

        if not src_doc and not tgt_doc:  # Both don't exist, which is fine
            return True
        if not src_doc or not tgt_doc:  # One exists but not the other
            return False

        # Compare documents using hash with custom JSON encoder
        src_hash = hashlib.md5(
            json.dumps(src_doc, sort_keys=True, cls=MongoJSONEncoder).encode()
        ).hexdigest()
        tgt_hash = hashlib.md5(
            json.dumps(tgt_doc, sort_keys=True, cls=MongoJSONEncoder).encode()
        ).hexdigest()
        return src_hash == tgt_hash
    except Exception as e:
        logger.error(f"Error verifying document {doc_id}: {e}")
        return False


def get_mongo_client(uri, db_name):
    """Create MongoDB client with retry logic"""
    for attempt in range(RETRY_LIMIT):
        try:
            client = MongoClient(uri, **MONGO_CLIENT_OPTIONS)
            # Test connection
            client.admin.command("ping")
            return client[db_name]
        except errors.ConnectionFailure as e:
            if attempt < RETRY_LIMIT - 1:
                logger.warning(
                    f"Connection attempt {attempt+1} failed: {e}. Retrying..."
                )
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                logger.error(f"Failed to connect after {RETRY_LIMIT} attempts: {e}")
                raise


def handle_deletes(
    src_collection, tgt_collection, collection_name, last_operation_time, force_check=False
):
    """Handle document deletions by checking for documents in target that no longer exist in source"""
    try:
        # Check if target has more documents than source
        src_count = src_collection.count_documents({})
        tgt_count = tgt_collection.count_documents({})
        
        # If target has more documents, we need to be more aggressive
        target_exceeds = tgt_count > src_count
        
        # Determine sample size - check more documents in force mode or when target exceeds source
        sample_size = 1000 if (force_check or target_exceeds) else 100
        logger.info(f"[{collection_name}] Checking for deletions with sample size {sample_size} (target_exceeds={target_exceeds})")
        
        # Get documents from target to check if they still exist in source
        sample_docs = list(tgt_collection.find().limit(sample_size))

        delete_ops = []
        for doc in sample_docs:
            if not src_collection.find_one({"_id": doc["_id"]}):
                delete_ops.append(DeleteOne({"_id": doc["_id"]}))
                logger.info(
                    f"Document {doc['_id']} deleted in source, removing from target"
                )

        if delete_ops:
            tgt_collection.bulk_write(delete_ops)
            logger.info(
                f"Removed {len(delete_ops)} deleted documents from {collection_name}"
            )

            # Update deletion count in checkpoint
            path = os.path.join(PROGRESS_DIR, f"{collection_name}_cdc.json")
            try:
                # Load existing checkpoint
                last_updated_at = None
                if os.path.exists(path):
                    with open(path) as f:
                        data = json.load(f)
                        last_updated_at = data.get("last_updated_at")

                # Save updated checkpoint with deletion count
                save_checkpoint(
                    collection_name,
                    last_updated_at,
                    last_operation_time,
                    0,
                    len(delete_ops),
                )
            except Exception as e:
                logger.error(f"Failed to update deletion count in checkpoint: {e}")

            return len(delete_ops)
        return 0
    except Exception as e:
        logger.error(f"Error handling deletes for {collection_name}: {e}")
        return 0


def cdc_migrate_collection(
    source_db,
    target_db,
    collection_name,
    batch_size=BATCH_SIZE,
    progress_dir=PROGRESS_DIR,
    handle_deletions=True,
    force_refresh=CDC_FORCE_REFRESH,
):
    """Migrate changes from source to target using polling approach"""
    logger.info(f"[CDC START] {collection_name}")

    try:
        # Get database connections with retry logic
        src_db = get_mongo_client(SOURCE_URI, source_db)
        dest_db = get_mongo_client(TARGET_URI, target_db)

        src_collection = src_db[collection_name]
        tgt_collection = dest_db[collection_name]

        # Load checkpoint (unless force refresh is enabled)
        last_updated_at, last_operation_time = (
            (None, None)
            if force_refresh
            else load_checkpoint(collection_name, progress_dir)
        )

        # Always check for target exceeding source (potential deletion case)
        try:
            src_count = src_collection.count_documents({})
            tgt_count = tgt_collection.count_documents({})
            target_exceeds = tgt_count > src_count
            
            if target_exceeds:
                logger.info(f"[{collection_name}] Target has more documents than source ({tgt_count} > {src_count}). Checking for deletions.")
                deletion_count = handle_deletes(src_collection, tgt_collection, collection_name, None, True)
                logger.info(f"[{collection_name}] Found and removed {deletion_count} deletions")
        except Exception as e:
            logger.error(f"[{collection_name}] Error checking document counts: {e}")

        if force_refresh:
            logger.info(
                f"[{collection_name}] Force refresh enabled, ignoring checkpoint"
            )
            # When forcing refresh, check all documents
            query = {}
            sort_field = "_id"  # Use _id for sorting in force refresh mode
            logger.info(f"[{collection_name}] Using empty query for full refresh")
        else:
            # Determine query based on available fields
            query = {}
            sort_field = "updatedAt"  # Default sort field

            # Check if collection has updatedAt field
            sample_doc = src_collection.find_one()
            if sample_doc and "updatedAt" in sample_doc:
                if last_updated_at:
                    # Convert string timestamp to datetime if needed
                    if isinstance(last_updated_at, str):
                        try:
                            last_updated_at = datetime.fromisoformat(
                                last_updated_at.replace("Z", "+00:00")
                            )
                        except ValueError:
                            logger.warning(
                                f"Could not parse last_updated_at: {last_updated_at}, using as-is"
                            )

                    query = {"updatedAt": {"$gt": last_updated_at}}
                    if CDC_DEBUG:
                        logger.info(
                            f"[{collection_name}] Using updatedAt query: {query}"
                        )
            else:
                # Fall back to _id if updatedAt not available
                sort_field = "_id"
                if last_operation_time:  # Use operation time as fallback
                    # This assumes _id is ObjectId which has timestamp embedded
                    query = {"_id": {"$gt": ObjectId(last_operation_time)}}
                    if CDC_DEBUG:
                        logger.info(f"[{collection_name}] Using _id query: {query}")
                logger.warning(
                    f"Collection {collection_name} doesn't have updatedAt field, using _id for CDC"
                )

        # Always check for document differences regardless of updatedAt
        if CDC_DEBUG:
            logger.info(
                f"[{collection_name}] Final query: {query}, sort field: {sort_field if 'sort_field' in locals() else 'None'}"
            )
            total_docs = src_collection.count_documents({})
            filtered_docs = src_collection.count_documents(query)
            logger.info(
                f"[{collection_name}] Total docs: {total_docs}, filtered docs: {filtered_docs}"
            )

        # Add a function to check for document differences regardless of timestamps
        def check_document_differences(src_doc, tgt_doc):
            """Compare documents and return True if they are different"""
            if not tgt_doc:  # Document doesn't exist in target
                return True

            # Compare full documents using hash
            src_hash = hashlib.md5(
                json.dumps(src_doc, sort_keys=True, cls=MongoJSONEncoder).encode()
            ).hexdigest()
            tgt_hash = hashlib.md5(
                json.dumps(tgt_doc, sort_keys=True, cls=MongoJSONEncoder).encode()
            ).hexdigest()

            if src_hash != tgt_hash:
                if CDC_DEBUG:
                    logger.info(
                        f"Document hashes differ: {src_doc['_id']} - src={src_hash}, tgt={tgt_hash}"
                    )
                return True
            return False

        # Initialize total_synced counter
        total_synced = 0

        # If force_refresh is enabled, check all collections more thoroughly
        if force_refresh:  # Apply to all collections in force-refresh mode
            logger.info(
                f"[{collection_name}] Performing targeted check for specific documents"
            )
            # Check documents in target to see if they need updating
            # Use a larger sample size in force-refresh mode
            sample_size = 500  # Increased sample size for better coverage
            logger.info(f"[{collection_name}] Performing targeted check with sample size {sample_size}")
            
            target_docs = list(tgt_collection.find().limit(sample_size))
            targeted_synced = 0
            targeted_ops = []
            
            for tgt_doc in target_docs:
                src_doc = src_collection.find_one({"_id": tgt_doc["_id"]})
                if src_doc and check_document_differences(src_doc, tgt_doc):
                    logger.info(f"[{collection_name}] Found difference in document {tgt_doc['_id']}, will update")
                    targeted_ops.append(ReplaceOne({"_id": tgt_doc["_id"]}, src_doc))
            
            # Bulk write all updates at once for better performance
            if targeted_ops:
                try:
                    result = tgt_collection.bulk_write(targeted_ops, ordered=False)
                    targeted_synced = len(targeted_ops)
                    
                    # Update checkpoint with these updates
                    save_checkpoint(
                        collection_name,
                        last_updated_at,
                        last_operation_time,
                        targeted_synced,
                        0,
                        progress_dir
                    )
                except Exception as e:
                    logger.error(f"[{collection_name}] Error in targeted updates: {e}")
            
            logger.info(f"[{collection_name}] Updated {targeted_synced} documents in targeted check")
            total_synced += targeted_synced

        # Normal CDC processing
        sort_field = (
            sort_field if "sort_field" in locals() else "_id"
        )  # Ensure sort_field is defined

        while True:
            # Fetch batch with retry logic
            for attempt in range(RETRY_LIMIT):
                try:
                    docs = list(
                        src_collection.find(query)
                        .sort(sort_field, pymongo.ASCENDING)
                        .limit(batch_size)
                    )
                    break
                except Exception as e:
                    if attempt < RETRY_LIMIT - 1:
                        logger.warning(
                            f"[{collection_name}] Fetch attempt {attempt+1} failed: {e}. Retrying..."
                        )
                        time.sleep(RETRY_DELAY * (attempt + 1))
                    else:
                        logger.error(
                            f"[{collection_name}] Error fetching docs after {RETRY_LIMIT} attempts: {e}"
                        )
                        raise

            if not docs:
                # Handle deletions if enabled - always check at the end of processing
                if handle_deletions:
                    # Check if target has more documents than source
                    src_count = src_collection.count_documents({})
                    tgt_count = tgt_collection.count_documents({})
                    target_exceeds = tgt_count > src_count
                    
                    if target_exceeds:
                        logger.info(f"[{collection_name}] Target has more documents than source ({tgt_count} > {src_count}). Forcing thorough deletion check.")
                    
                    logger.info(f"[{collection_name}] Checking for deletions at end of processing")
                    deletion_count = handle_deletes(
                        src_collection,
                        tgt_collection,
                        collection_name,
                        last_operation_time,
                        force_check=force_refresh or target_exceeds  # Force check when force_refresh is enabled or target exceeds source
                    )
                    logger.info(f"[{collection_name}] Found {deletion_count} deletions at end of processing")
                logger.info(
                    f"[CDC DONE] {collection_name} - Total synced: {total_synced}"
                )
                break

            # Process batch
            operations = []
            verification_failures = 0
            update_count = 0  # Initialize update counter

            for doc in docs:
                # Check if document needs to be migrated by comparing with target
                doc_id = doc["_id"]
                tgt_doc = tgt_collection.find_one({"_id": doc_id})

                # Always migrate if document doesn't exist in target
                if tgt_doc is None:
                    if CDC_DEBUG:
                        logger.info(
                            f"[{collection_name}] Document {doc_id} not found in target, will migrate"
                        )
                    operations.append(ReplaceOne({"_id": doc_id}, doc, upsert=True))
                    continue

                # For existing documents, first check content differences
                src_hash = hashlib.md5(
                    json.dumps(doc, sort_keys=True, cls=MongoJSONEncoder).encode()
                ).hexdigest()
                tgt_hash = hashlib.md5(
                    json.dumps(tgt_doc, sort_keys=True, cls=MongoJSONEncoder).encode()
                ).hexdigest()

                if src_hash != tgt_hash:
                    # Content differs, check if we should update based on timestamps
                    update_needed = True

                    # If both have updatedAt fields, check which is newer
                    if "updatedAt" in doc and "updatedAt" in tgt_doc:
                        src_updated = doc["updatedAt"]
                        tgt_updated = tgt_doc["updatedAt"]

                        # Convert to datetime objects if they're strings
                        if isinstance(src_updated, str):
                            try:
                                src_updated = datetime.fromisoformat(
                                    src_updated.replace("Z", "+00:00")
                                )
                            except ValueError:
                                logger.warning(
                                    f"Could not parse source updatedAt: {src_updated}"
                                )
                                # Keep as string if parsing fails

                        if isinstance(tgt_updated, str):
                            try:
                                tgt_updated = datetime.fromisoformat(
                                    tgt_updated.replace("Z", "+00:00")
                                )
                            except ValueError:
                                logger.warning(
                                    f"Could not parse target updatedAt: {tgt_updated}"
                                )
                                # Keep as string if parsing fails

                        # Debug log the comparison
                        logger.info(
                            f"[{collection_name}] Comparing timestamps for {doc_id}: src={src_updated}, tgt={tgt_updated}"
                        )

                        # String comparison if one couldn't be parsed to datetime
                        if isinstance(src_updated, str) or isinstance(tgt_updated, str):
                            update_needed = str(src_updated) > str(tgt_updated)
                        else:
                            update_needed = src_updated > tgt_updated

                    if update_needed or force_refresh:
                        if CDC_DEBUG:
                            logger.info(
                                f"[{collection_name}] Document {doc_id} content differs, will migrate"
                            )
                        operations.append(ReplaceOne({"_id": doc_id}, doc, upsert=True))
                    elif CDC_DEBUG:
                        logger.info(
                            f"[{collection_name}] Document {doc_id} content differs but source not newer, skipping"
                        )
                elif CDC_DEBUG:
                    logger.info(
                        f"[{collection_name}] Document {doc_id} content identical, skipping"
                    )

            # Write batch with retry logic
            update_count = 0
            if operations:
                for attempt in range(RETRY_LIMIT):
                    try:
                        result = tgt_collection.bulk_write(operations, ordered=False)
                        update_count = len(operations)
                        break
                    except Exception as e:
                        if attempt < RETRY_LIMIT - 1:
                            logger.warning(
                                f"[{collection_name}] Write attempt {attempt+1} failed: {e}. Retrying..."
                            )
                            time.sleep(RETRY_DELAY * (attempt + 1))
                        else:
                            logger.error(
                                f"[{collection_name}] Write error after {RETRY_LIMIT} attempts: {e}"
                            )
                            # Log individual documents for manual recovery
                            for doc in docs:
                                log_failed_doc(
                                    collection_name, doc, f"Bulk write failed: {e}"
                                )
                            raise

            # Verify a sample of documents
            sample_size = min(10, len(docs))  # Verify up to 10 docs per batch
            for i in range(sample_size):
                doc_index = i * (len(docs) // sample_size) if sample_size > 1 else 0
                if doc_index < len(docs):
                    doc_id = docs[doc_index]["_id"]
                    if not verify_doc(src_collection, tgt_collection, doc_id):
                        verification_failures += 1
                        logger.warning(
                            f"[{collection_name}] Verification failed for document {doc_id}"
                        )

            if verification_failures > 0:
                logger.error(
                    f"[{collection_name}] {verification_failures}/{sample_size} documents failed verification"
                )

            # Update progress
            last_doc = docs[-1]
            if sort_field == "updatedAt":
                last_updated_at = last_doc["updatedAt"]
                # Ensure we're using the actual datetime object for comparison
                if isinstance(last_updated_at, str):
                    try:
                        last_updated_at = datetime.fromisoformat(
                            last_updated_at.replace("Z", "+00:00")
                        )
                    except ValueError:
                        logger.warning(
                            f"Could not parse updatedAt: {last_updated_at}, using as-is"
                        )

                query = {"updatedAt": {"$gt": last_updated_at}}
            else:
                last_operation_time = str(last_doc["_id"])
                query = {"_id": {"$gt": last_doc["_id"]}}

            save_checkpoint(
                collection_name,
                last_updated_at,
                last_operation_time,
                update_count,
                0,
                progress_dir,
            )
            total_synced += len(docs)

            logger.info(
                f"[CDC PROGRESS] {collection_name}: Synced {total_synced} documents"
            )

    except Exception as e:
        logger.error(f"CDC migration failed for {collection_name}: {e}", exc_info=True)
        raise


def cdc_migrate_all_collections(
    source_db,
    target_db,
    collection_names,
    batch_size=BATCH_SIZE,
    progress_dir=PROGRESS_DIR,
    handle_deletions=True,
    force_refresh=CDC_FORCE_REFRESH,
    threads=None,
):
    """Run CDC migration for multiple collections using polling approach

    Args:
        source_db: Source database name
        target_db: Target database name
        collection_names: List of collection names to migrate
        batch_size: Number of documents to process in each batch
        progress_dir: Directory to store progress files
        handle_deletions: Whether to check for and handle deleted documents (always True in practice)
        force_refresh: Whether to ignore checkpoints and scan all documents
        threads: Number of threads to use (None=sequential, 0=auto/one per collection, N=specific number)
    """
    # Always enable real-time update and deletion handling
    handle_deletions = True
    # If threads is None, run sequentially (original behavior)
    if threads is None:
        return cdc_migrate_sequential(
            source_db,
            target_db,
            collection_names,
            batch_size,
            progress_dir,
            handle_deletions,
            force_refresh,
        )
    else:
        return cdc_migrate_threaded(
            source_db,
            target_db,
            collection_names,
            batch_size,
            progress_dir,
            handle_deletions,
            force_refresh,
            threads,
        )


def cdc_migrate_sequential(
    source_db,
    target_db,
    collection_names,
    batch_size=BATCH_SIZE,
    progress_dir=PROGRESS_DIR,
    handle_deletions=True,
    force_refresh=CDC_FORCE_REFRESH,
):
    """Run CDC migration sequentially for multiple collections"""
    results = {"success": [], "failed": []}

    for collection_name in collection_names:
        logger.info(f"Starting CDC polling sync for: {collection_name}")
        try:
            cdc_migrate_collection(
                source_db,
                target_db,
                collection_name,
                batch_size=batch_size,
                progress_dir=progress_dir,
                handle_deletions=handle_deletions,
                force_refresh=force_refresh,
            )
            results["success"].append(collection_name)
        except Exception as e:
            logger.error(
                f"Failed CDC polling sync for {collection_name}: {e}", exc_info=True
            )
            results["failed"].append({"collection": collection_name, "error": str(e)})

    # Log summary
    logger.info(
        f"CDC polling cycle complete. Success: {len(results['success'])}, Failed: {len(results['failed'])}"
    )

    if results["failed"]:
        logger.error(f"Failed collections: {results['failed']}")

    return results


def cdc_worker_thread(
    source_db,
    target_db,
    collection_name,
    batch_size,
    progress_dir,
    handle_deletions,
    force_refresh,
    shutdown_event=None,
):
    """Worker thread function for CDC migration of a single collection"""
    logger.info(f"[THREAD] Starting CDC thread for collection: {collection_name}")
    
    # Always enable real-time update and deletion handling
    handle_deletions = True
    
    # Always use force_refresh for the first cycle to ensure proper sync
    first_cycle = True

    try:
        while not (shutdown_event and shutdown_event.is_set()):
            try:
                # Run one CDC cycle for this collection
                # Use force_refresh for the first cycle to ensure proper detection
                current_force_refresh = True if first_cycle else force_refresh
                
                logger.info(f"[THREAD] Running CDC cycle for {collection_name} with force_refresh={current_force_refresh}")
                
                cdc_migrate_collection(
                    source_db,
                    target_db,
                    collection_name,
                    batch_size=batch_size,
                    progress_dir=progress_dir,
                    handle_deletions=True,  # Always handle deletions in threaded mode
                    force_refresh=current_force_refresh,
                )
                
                # After first cycle, set flag to false
                if first_cycle:
                    first_cycle = False
                    logger.info(f"[THREAD] First cycle completed for {collection_name}, subsequent cycles will use normal mode")

                # Sleep for a short interval before checking again
                # Use small intervals to check shutdown_event frequently
                for _ in range(
                    POLLING_INTERVAL
                    if hasattr(sys.modules["config.config"], "POLLING_INTERVAL")
                    else 5
                ):
                    if shutdown_event and shutdown_event.is_set():
                        break
                    time.sleep(1)

            except Exception as e:
                logger.error(
                    f"[THREAD] Error in CDC thread for {collection_name}: {e}",
                    exc_info=True,
                )
                # Sleep before retry on error
                time.sleep(5)

    except Exception as e:
        logger.error(
            f"[THREAD] Fatal error in CDC thread for {collection_name}: {e}",
            exc_info=True,
        )

    logger.info(f"[THREAD] CDC thread for collection {collection_name} stopped")
    return collection_name


def cdc_migrate_threaded(
    source_db,
    target_db,
    collection_names,
    batch_size=BATCH_SIZE,
    progress_dir=PROGRESS_DIR,
    handle_deletions=True,
    force_refresh=CDC_FORCE_REFRESH,
    threads=0,
):
    """Run CDC migration with multiple threads - one per collection or a specified number

    Args:
        threads: Number of threads (0=one per collection, N=specific number)
    """
    results = {"success": [], "failed": []}
    shutdown_event = threading.Event()

    # Determine number of threads to use
    if threads == 0:  # Auto mode - one thread per collection
        max_workers = len(collection_names)
        logger.info(
            f"Using auto threading mode with {max_workers} threads (one per collection)"
        )
    else:  # Specific number of threads
        max_workers = min(threads, len(collection_names))
        logger.info(
            f"Using {max_workers} threads for {len(collection_names)} collections"
        )

    try:
        # Create thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Start threads for each collection
            future_to_collection = {}

            for collection_name in collection_names:
                future = executor.submit(
                    cdc_worker_thread,
                    source_db,
                    target_db,
                    collection_name,
                    batch_size,
                    progress_dir,
                    handle_deletions,
                    force_refresh,
                    shutdown_event,
                )
                future_to_collection[future] = collection_name

            # Wait for all threads to complete or for shutdown
            try:
                # This will block until all threads complete or an exception occurs
                for future in concurrent.futures.as_completed(future_to_collection):
                    collection_name = future_to_collection[future]
                    try:
                        # Get the result (will raise exception if thread failed)
                        result = future.result()
                        results["success"].append(collection_name)
                    except Exception as e:
                        logger.error(f"CDC thread for {collection_name} failed: {e}")
                        results["failed"].append(
                            {"collection": collection_name, "error": str(e)}
                        )

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received, shutting down threads...")
                shutdown_event.set()
                # Wait for threads to finish gracefully
                for future in concurrent.futures.as_completed(future_to_collection):
                    try:
                        future.result(
                            timeout=10
                        )  # Give threads 10 seconds to shut down
                    except concurrent.futures.TimeoutError:
                        logger.warning(
                            f"Thread for {future_to_collection[future]} did not shut down gracefully"
                        )
                    except Exception as e:
                        logger.error(f"Error during thread shutdown: {e}")

    except Exception as e:
        logger.error(f"Error in threaded CDC migration: {e}", exc_info=True)
        # Signal all threads to shut down
        shutdown_event.set()

    # Log summary
    logger.info(
        f"Threaded CDC complete. Success: {len(results['success'])}, Failed: {len(results['failed'])}"
    )
    if results["failed"]:
        logger.error(f"Failed collections: {results['failed']}")

    return results
