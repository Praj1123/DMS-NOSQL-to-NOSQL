import pymongo
import json
import os
import time
import threading
from datetime import datetime
from pymongo import MongoClient, ReplaceOne, DeleteOne, errors
from pymongo.errors import PyMongoError
from config.config import *
from .collection_worker import MongoJSONEncoder

# Setup logger
logger = setup_logging("change_stream_worker")

def load_resume_token(collection_name, progress_dir=PROGRESS_DIR):
    """Load resume token for change stream"""
    path = os.path.join(progress_dir, f"{collection_name}_resume_token.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
                return data.get("resume_token")
        except json.JSONDecodeError:
            logger.error(f"Corrupted resume token file for {collection_name}, starting fresh")
            return None
    return None

def save_resume_token(collection_name, resume_token, progress_dir=PROGRESS_DIR):
    """Save resume token for change stream"""
    try:
        path = os.path.join(progress_dir, f"{collection_name}_resume_token.json")
        # Atomic write by using temporary file
        temp_path = f"{path}.tmp"
        with open(temp_path, "w") as f:
            json.dump({
                "resume_token": resume_token,
                "timestamp": datetime.now().isoformat()
            }, f, cls=MongoJSONEncoder)
        os.replace(temp_path, path)  # Atomic replacement
    except Exception as e:
        logger.error(f"Failed to save resume token for {collection_name}: {e}")

def process_change_event(change, tgt_collection, collection_name):
    """Process a single change event from the change stream"""
    try:
        operation_type = change.get("operationType")
        
        if operation_type == "insert" or operation_type == "update" or operation_type == "replace":
            # Get the full document
            doc = change.get("fullDocument")
            if doc:
                tgt_collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                logger.debug(f"[{collection_name}] Applied {operation_type} for document {doc['_id']}")
            else:
                logger.warning(f"[{collection_name}] Missing fullDocument in {operation_type} event")
        
        elif operation_type == "delete":
            # Get document key (usually just _id)
            doc_key = change.get("documentKey")
            if doc_key and "_id" in doc_key:
                tgt_collection.delete_one({"_id": doc_key["_id"]})
                logger.debug(f"[{collection_name}] Applied delete for document {doc_key['_id']}")
            else:
                logger.warning(f"[{collection_name}] Missing documentKey in delete event")
        
        # Return True for successful processing
        return True
    
    except Exception as e:
        logger.error(f"[{collection_name}] Error processing change event: {e}")
        return False

def watch_collection_changes(
    source_db,
    target_db,
    collection_name,
    shutdown_event,
    batch_size=100,
    progress_dir=PROGRESS_DIR
):
    """Watch for changes in a collection using MongoDB change streams"""
    logger.info(f"[CHANGE STREAM START] Watching collection: {collection_name}")
    
    # Get database connections
    try:
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)
        
        src_db = src_client[source_db]
        tgt_db = tgt_client[target_db]
        
        src_collection = src_db[collection_name]
        tgt_collection = tgt_db[collection_name]
        
        # Load resume token if available
        resume_token = load_resume_token(collection_name, progress_dir)
        
        # Pipeline to include full document on updates
        pipeline = [
            {"$match": {"operationType": {"$in": ["insert", "update", "replace", "delete"]}}}
        ]
        
        # Change stream options
        options = {
            "full_document": "updateLookup",  # Include the full document on updates
            "batch_size": batch_size
        }
        
        # Add resume token if available
        if resume_token:
            options["resume_after"] = resume_token
            logger.info(f"[{collection_name}] Resuming change stream from saved token")
        else:
            logger.info(f"[{collection_name}] Starting new change stream")
        
        # Start change stream
        with src_collection.watch(pipeline, **options) as stream:
            logger.info(f"[{collection_name}] Change stream established successfully")
            
            # Track statistics
            stats = {
                "processed": 0,
                "errors": 0,
                "last_processed": None
            }
            
            # Process changes in batches
            while not shutdown_event.is_set():
                try:
                    # Try to get next change with timeout
                    change = stream.try_next()
                    
                    # If no change is available, wait a bit and try again
                    if change is None:
                        time.sleep(0.1)  # Short sleep to prevent CPU spinning
                        continue
                    
                    # Process the change
                    success = process_change_event(change, tgt_collection, collection_name)
                    
                    # Update statistics
                    if success:
                        stats["processed"] += 1
                    else:
                        stats["errors"] += 1
                    
                    stats["last_processed"] = datetime.now().isoformat()
                    
                    # Save resume token periodically (every 100 changes)
                    if stats["processed"] % 100 == 0:
                        resume_token = stream.resume_token
                        save_resume_token(collection_name, resume_token, progress_dir)
                        logger.info(f"[{collection_name}] Processed {stats['processed']} changes, saved resume token")
                    
                except PyMongoError as e:
                    logger.error(f"[{collection_name}] MongoDB error in change stream: {e}")
                    # For most MongoDB errors, we should try to reconnect
                    time.sleep(5)  # Wait before reconnecting
                    break  # Break out of the loop to reconnect
                    
                except Exception as e:
                    logger.error(f"[{collection_name}] Unexpected error in change stream: {e}", exc_info=True)
                    time.sleep(5)  # Wait before continuing
            
            # Save final resume token before exiting
            if not shutdown_event.is_set():
                try:
                    resume_token = stream.resume_token
                    save_resume_token(collection_name, resume_token, progress_dir)
                    logger.info(f"[{collection_name}] Final resume token saved")
                except Exception as e:
                    logger.error(f"[{collection_name}] Error saving final resume token: {e}")
        
        logger.info(f"[CHANGE STREAM END] {collection_name} - Processed {stats['processed']} changes with {stats['errors']} errors")
    
    except Exception as e:
        logger.error(f"[{collection_name}] Change stream worker failed: {e}", exc_info=True)
    
    finally:
        # Clean up connections
        if 'src_client' in locals():
            src_client.close()
        if 'tgt_client' in locals():
            tgt_client.close()

def run_change_streams(collections, shutdown_event):
    """Run change streams for multiple collections in parallel"""
    threads = []
    
    for c in collections:
        thread = threading.Thread(
            target=watch_collection_changes,
            args=(
                c["source_db"],
                c["target_db"],
                c["collection"],
                shutdown_event
            ),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        logger.info(f"Started change stream thread for {c['collection']}")
    
    # Wait for all threads to complete (they'll only complete if shutdown is requested)
    for thread in threads:
        thread.join()
    
    logger.info("All change stream threads have terminated")