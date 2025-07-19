import json
import os
import sys
import time
import signal
import hashlib
import threading
import concurrent.futures
import webbrowser
import argparse
from datetime import datetime
from pymongo import MongoClient
from workers.collection_worker import migrate_collection, MongoJSONEncoder
from workers.cdc_worker import cdc_migrate_all_collections
from workers.change_stream_worker import run_change_streams
from report_generator import generate_html_report, format_migrate_report, format_verify_report, format_update_report
from config.config import *

# Setup logger
logger = setup_logging("migrate")

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(sig, frame):
    """Handle Ctrl+C and other termination signals"""
    global shutdown_requested
    logger.info("Shutdown requested. Completing current batch before exiting...")
    shutdown_requested = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def validate_connections():
    """Validate database connections before starting migration"""
    try:
        # Check source connection
        logger.info("Validating source database connection...")
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        src_client.admin.command('ping')
        
        # Check target connection
        logger.info("Validating target database connection...")
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client.admin.command('ping')
        
        logger.info("Database connections validated successfully")
        return True
    except Exception as e:
        logger.error(f"Database connection validation failed: {e}")
        return False

def load_collections():
    """Load collections configuration with validation"""
    try:
        with open("collections.json") as f:
            collections = json.load(f)
        
        # Validate collection configuration
        for i, c in enumerate(collections):
            if not all(k in c for k in ["source_db", "target_db", "collection"]):
                logger.error(f"Invalid collection config at index {i}: {c}")
                return None
        
        logger.info(f"Loaded {len(collections)} collections for migration")
        return collections
    except Exception as e:
        logger.error(f"Failed to load collections: {e}")
        return None

def run_job(c):
    """Run a single collection migration job with error handling"""
    if shutdown_requested:
        return {"status": "skipped", "collection": c["collection"]}
    
    try:
        logger.info(f"Starting migration for {c['collection']}")
        start_time = time.time()
        
        migrate_collection(c["source_db"], c["target_db"], c["collection"])
        
        duration = time.time() - start_time
        logger.info(f"Migration completed for {c['collection']} in {duration:.2f} seconds")
        
        return {"status": "success", "collection": c["collection"], "duration": duration}
    except Exception as e:
        logger.error(f"Migration failed for {c['collection']}: {e}", exc_info=True)
        return {"status": "failed", "collection": c["collection"], "error": str(e)}

def verify_migration(collections):
    """Verify migration results by comparing collection stats"""
    logger.info("Starting migration verification...")
    verification_results = []
    
    try:
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)
        
        for c in collections:
            src_coll = src_client[c["source_db"]][c["collection"]]
            tgt_coll = tgt_client[c["target_db"]][c["collection"]]
            
            src_count = src_coll.count_documents({})
            tgt_count = tgt_coll.count_documents({})
            match_percentage = (tgt_count / src_count * 100) if src_count > 0 else 0
            
            result = {
                "collection": c["collection"],
                "source_count": src_count,
                "target_count": tgt_count,
                "match_percentage": match_percentage,
                "status": "OK" if match_percentage >= 99.9 else "MISMATCH"
            }
            
            verification_results.append(result)
            logger.info(f"Verification {result['status']} for {c['collection']}: "
                       f"{tgt_count}/{src_count} documents ({match_percentage:.2f}%)")
    
    except Exception as e:
        logger.error(f"Verification error: {e}")
    
    # Save verification results
    try:
        os.makedirs(VERIFICATION_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(os.path.join(VERIFICATION_DIR, f"verification_{timestamp}.json"), "w") as f:
            json.dump(verification_results, f, indent=2, cls=MongoJSONEncoder)
    except Exception as e:
        logger.error(f"Failed to save verification results: {e}")
    
    return verification_results

def run_cdc_mode_legacy(collections, threads=None, batch_size=BATCH_SIZE, force_refresh=CDC_FORCE_REFRESH):
    """Legacy CDC mode using polling instead of change streams"""
    thread_mode = "sequential"
    if threads is not None:
        thread_mode = f"threaded ({threads} threads)" if threads > 0 else "threaded (auto)"
    
    logger.info(f"Starting legacy CDC mode with polling in {thread_mode} mode")
    
    try:
        while not shutdown_requested:
            for db_group in set((c["source_db"], c["target_db"]) for c in collections):
                source_db, target_db = db_group
                collection_names = [c["collection"] for c in collections 
                                  if c["source_db"] == source_db and c["target_db"] == target_db]
                
                logger.info(f"Running CDC sync for {len(collection_names)} collections in {source_db}")
                cdc_migrate_all_collections(
                    source_db, 
                    target_db, 
                    collection_names, 
                    batch_size=batch_size,
                    force_refresh=force_refresh,
                    threads=threads
                )
            
            # If using threads, we don't need to sleep here as each thread manages its own polling interval
            if threads is None:
                logger.info("CDC cycle complete, sleeping before next sync")
                # Sleep for a while before next CDC cycle
                for _ in range(POLLING_INTERVAL if hasattr(sys.modules['config.config'], 'POLLING_INTERVAL') else 60):  
                    if shutdown_requested:
                        break
                    time.sleep(1)
            else:
                # For threaded mode, just check for shutdown periodically
                while not shutdown_requested:
                    time.sleep(1)
    
    except Exception as e:
        logger.error(f"CDC mode error: {e}", exc_info=True)
    
    logger.info("CDC mode stopped")

def run_cdc_mode(collections, threads=None, batch_size=BATCH_SIZE, force_refresh=CDC_FORCE_REFRESH):
    """Run continuous CDC mode to keep databases in sync"""
    thread_info = ""
    if threads is not None:
        thread_info = f" with {'auto-threading' if threads == 0 else f'{threads} threads'}"
    
    logger.info(f"Starting CDC mode{thread_info}")
    
    try:
        # Try to use change streams first
        try:
            # Test if change streams are supported
            src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
            src_db = src_client[collections[0]["source_db"]]
            src_collection = src_db[collections[0]["collection"]]
            
            # Try to create a change stream with a small timeout
            with src_collection.watch(max_await_time_ms=1000) as _:
                logger.info("Change streams are supported, using real-time monitoring")
                
                # Create a threading event for signaling shutdown to all threads
                shutdown_event = threading.Event()
                
                # Start change stream monitoring in separate threads
                run_change_streams(collections, shutdown_event)
                
                # Keep the main thread running until shutdown is requested
                while not shutdown_requested:
                    time.sleep(1)
                
                # Signal all change stream threads to shut down
                logger.info("Signaling change stream threads to shut down")
                shutdown_event.set()
                
        except Exception as e:
            logger.warning(f"Change streams not supported: {e}. Falling back to polling mode.")
            run_cdc_mode_legacy(collections, threads, batch_size, force_refresh)
    
    except Exception as e:
        logger.error(f"CDC mode error: {e}", exc_info=True)
    
    logger.info("CDC mode stopped")

def update_collections(collections, batch_size=100):
    """Update changed documents in target database"""
    logger.info("Starting update operation to sync changed documents")
    
    try:
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)
        
        total_updated = 0
        
        for c in collections:
            source_db = c["source_db"]
            target_db = c["target_db"]
            collection_name = c["collection"]
            
            logger.info(f"Checking for updates in {collection_name}")
            
            src_collection = src_client[source_db][collection_name]
            tgt_collection = tgt_client[target_db][collection_name]
            
            # Process documents in batches to avoid memory issues
            updated_count = 0
            total_docs = src_collection.count_documents({})
            logger.info(f"Found {total_docs} documents in source collection {collection_name}")
            
            # Use cursor instead of loading all documents at once
            cursor = src_collection.find({}).batch_size(batch_size)
            batch_num = 0
            
            for doc in cursor:
                if batch_num % batch_size == 0:
                    logger.info(f"Processing batch {batch_num//batch_size + 1} in {collection_name}")
                
                doc_id = doc["_id"]
                tgt_doc = tgt_collection.find_one({"_id": doc_id})
                
                # If document doesn't exist in target, insert it
                if tgt_doc is None:
                    tgt_collection.insert_one(doc)
                    updated_count += 1
                    batch_num += 1
                    continue
                
                # Check if document has been updated
                if "updatedAt" in doc and "updatedAt" in tgt_doc:
                    src_updated = doc["updatedAt"]
                    tgt_updated = tgt_doc["updatedAt"]
                    
                    # Convert to datetime objects if they're strings
                    if isinstance(src_updated, str):
                        try:
                            src_updated = datetime.fromisoformat(src_updated.replace('Z', '+00:00'))
                        except ValueError:
                            pass
                    
                    if isinstance(tgt_updated, str):
                        try:
                            tgt_updated = datetime.fromisoformat(tgt_updated.replace('Z', '+00:00'))
                        except ValueError:
                            pass
                    
                    # Update if source is newer
                    if src_updated > tgt_updated:
                        tgt_collection.replace_one({"_id": doc_id}, doc)
                        updated_count += 1
                else:
                    # If no updatedAt field, compare content
                    src_hash = hashlib.md5(json.dumps(doc, sort_keys=True, cls=MongoJSONEncoder).encode()).hexdigest()
                    tgt_hash = hashlib.md5(json.dumps(tgt_doc, sort_keys=True, cls=MongoJSONEncoder).encode()).hexdigest()
                    
                    if src_hash != tgt_hash:
                        tgt_collection.replace_one({"_id": doc_id}, doc)
                        updated_count += 1
                
                batch_num += 1
                
                # Add a small delay every 1000 documents to prevent overwhelming the server
                if batch_num % 1000 == 0:
                    time.sleep(0.1)
            
            logger.info(f"Updated {updated_count} documents in {collection_name}")
            total_updated += updated_count
        
        logger.info(f"Update operation complete. Total documents updated: {total_updated}")
        return total_updated
    
    except Exception as e:
        logger.error(f"Update operation failed: {e}", exc_info=True)
        return -1

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="MongoDB Migration Tool")
    parser.add_argument("mode", nargs="?", default="migrate", 
                        choices=["migrate", "cdc", "verify", "update"],
                        help="Operation mode: migrate, cdc, verify, or update")
    parser.add_argument("--threads", type=str, default=None,
                        help="Number of threads for CDC mode (auto or number)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help="Batch size for document processing")
    parser.add_argument("--force-refresh", action="store_true",
                        help="Force refresh ignoring checkpoints")
    
    return parser.parse_args()

def main():
    """Main function to run the migration process"""
    logger.info("=== MongoDB Migration Tool Starting ===")
    
    # Parse command line arguments
    args = parse_arguments()
    mode = args.mode.lower()
    
    # Process thread argument
    threads = None
    if args.threads:
        if args.threads.lower() == "auto":
            threads = 0  # Auto mode - one thread per collection
        else:
            try:
                threads = int(args.threads)
            except ValueError:
                logger.error(f"Invalid thread count: {args.threads}. Using default.")
    
    # Validate connections
    if not validate_connections():
        logger.error("Connection validation failed. Exiting.")
        return 1
    
    # Load collections
    collections = load_collections()
    if not collections:
        logger.error("Failed to load collections. Exiting.")
        return 1
    
    # Create required directories
    for directory in [PROGRESS_DIR, LOG_DIR, VERIFICATION_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    if mode == "migrate":
        # Run initial migration
        logger.info(f"Starting migration with {CONCURRENCY} workers")
        results = {"success": [], "failed": []}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            future_to_collection = {executor.submit(run_job, c): c for c in collections}
            
            for future in concurrent.futures.as_completed(future_to_collection):
                result = future.result()
                if result["status"] == "success":
                    results["success"].append(result["collection"])
                elif result["status"] == "failed":
                    results["failed"].append(result)
        
        # Log summary
        logger.info(f"Migration complete. Success: {len(results['success'])}, Failed: {len(results['failed'])}")
        if results["failed"]:
            logger.error(f"Failed collections: {results['failed']}")
        
        # Run update operation to catch any changed documents
        logger.info("Running update operation to catch any changed documents...")
        update_result = update_collections(collections)
        if update_result > 0:
            logger.info(f"Updated {update_result} documents during post-migration sync")
        
        # Verify migration
        verification_results = verify_migration(collections)
        failed_verifications = [r for r in verification_results if r["status"] != "OK"]
        
        if failed_verifications:
            logger.warning(f"{len(failed_verifications)} collections failed verification")
        else:
            logger.info("All collections verified successfully")
        
        # Generate HTML report
        report_data = format_migrate_report(results, update_result)
        report_file = generate_html_report(report_data, "migrate")
        logger.info(f"Migration report generated: {report_file}")
        
        # Open the report in the default browser
        try:
            webbrowser.open(f"file://{os.path.abspath(report_file)}")
        except Exception as e:
            logger.warning(f"Could not open report in browser: {e}")
        
        return 0 if not results["failed"] and not failed_verifications else 1
    
    elif mode == "cdc":
        # Run CDC mode
        batch_size = args.batch_size if hasattr(args, 'batch_size') else BATCH_SIZE
        force_refresh = args.force_refresh if hasattr(args, 'force_refresh') else CDC_FORCE_REFRESH
        
        run_cdc_mode(collections, threads, batch_size, force_refresh)
        
        # Generate CDC report after completion
        try:
            logger.info("Generating CDC report...")
            # Import here to avoid circular imports
            from generate_cdc_report import generate_cdc_report
            generate_cdc_report()
            logger.info("CDC report generated successfully")
        except Exception as e:
            logger.error(f"Failed to generate CDC report: {e}")
        
        return 0
    
    elif mode == "verify":
        # Run verification only
        verification_results = verify_migration(collections)
        failed_verifications = [r for r in verification_results if r["status"] != "OK"]
        return 0 if not failed_verifications else 1
    
    elif mode == "update":
        # Run update operation to sync changed documents
        result = update_collections(collections)
        return 0 if result >= 0 else 1
    
    else:
        logger.error(f"Unknown mode: {mode}. Valid modes are: migrate, cdc, verify, update")
        return 1

if __name__ == "__main__":
    sys.exit(main())