from pymongo import MongoClient, ReplaceOne, errors
import json, os, time, hashlib
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128
from datetime import datetime
from config.config import *

# Setup logger
logger = setup_logging("collection_worker")

def load_checkpoint(collection_name):
    path = os.path.join(PROGRESS_DIR, f"{collection_name}.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f).get("last_id")
        except json.JSONDecodeError:
            logger.error(f"Corrupted checkpoint file for {collection_name}, starting fresh")
            return None
    return None

def save_checkpoint(collection_name, last_id, count=0):
    try:
        path = os.path.join(PROGRESS_DIR, f"{collection_name}.json")
        # Atomic write by using temporary file
        temp_path = f"{path}.tmp"
        with open(temp_path, "w") as f:
            json.dump({
                "last_id": str(last_id),
                "count": count,
                "timestamp": time.time()
            }, f, cls=MongoJSONEncoder)
        os.replace(temp_path, path)  # Atomic replacement
    except Exception as e:
        logger.error(f"Failed to save checkpoint for {collection_name}: {e}")

# Custom JSON encoder for MongoDB types
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal128):
            return str(obj)
        if isinstance(obj, bytes):
            return obj.hex()
        return super().default(obj)

def verify_batch(src_docs, dest_collection, collection_name):
    """Verify migrated documents match source documents"""
    verification_errors = []
    doc_ids = [doc["_id"] for doc in src_docs]
    
    # Get the migrated documents from destination
    migrated_docs = list(dest_collection.find({"_id": {"$in": doc_ids}}))
    migrated_docs_dict = {doc["_id"]: doc for doc in migrated_docs}
    
    for src_doc in src_docs:
        doc_id = src_doc["_id"]
        if doc_id not in migrated_docs_dict:
            verification_errors.append(f"Document {doc_id} missing in destination")
            continue
            
        dest_doc = migrated_docs_dict[doc_id]
        
        # Compare documents using custom JSON encoder for MongoDB types
        src_hash = hashlib.md5(json.dumps(src_doc, sort_keys=True, cls=MongoJSONEncoder).encode()).hexdigest()
        dest_hash = hashlib.md5(json.dumps(dest_doc, sort_keys=True, cls=MongoJSONEncoder).encode()).hexdigest()
        
        if src_hash != dest_hash:
            verification_errors.append(f"Document {doc_id} content mismatch")
    
    # Log verification results
    if verification_errors:
        logger.error(f"Verification failed for {len(verification_errors)} documents in {collection_name}")
        with open(os.path.join(VERIFICATION_DIR, f"{collection_name}_errors.log"), "a") as f:
            for error in verification_errors:
                f.write(f"{error}\n")
        return False
    return True

def get_mongo_client(uri, db_name):
    """Create MongoDB client with retry logic"""
    for attempt in range(RETRY_LIMIT):
        try:
            client = MongoClient(uri, **MONGO_CLIENT_OPTIONS)
            # Test connection
            client.admin.command('ping')
            return client[db_name]
        except errors.ConnectionFailure as e:
            if attempt < RETRY_LIMIT - 1:
                logger.warning(f"Connection attempt {attempt+1} failed: {e}. Retrying...")
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                logger.error(f"Failed to connect after {RETRY_LIMIT} attempts: {e}")
                raise

def migrate_collection(source_db, target_db, collection_name):
    logger.info(f"[START] Migrating collection: {collection_name}")
    
    try:
        # Get database connections with retry logic
        src_db = get_mongo_client(SOURCE_URI, source_db)
        dest_db = get_mongo_client(TARGET_URI, target_db)
        
        src = src_db[collection_name]
        dest = dest_db[collection_name]
        
        # Create indexes in destination to match source
        try:
            src_indexes = src.index_information()
            for idx_name, idx_info in src_indexes.items():
                if idx_name != '_id_':  # Skip default _id index
                    keys = idx_info['key']
                    options = {k: v for k, v in idx_info.items() 
                              if k not in ['key', 'ns', 'v']}
                    try:
                        dest.create_index(keys, **options)
                        logger.info(f"Created index {idx_name} on {collection_name}")
                    except Exception as e:
                        logger.warning(f"Could not create index {idx_name}: {e}")
        except Exception as e:
            logger.warning(f"Could not copy indexes for {collection_name}: {e}")
        
        # Load checkpoint and prepare query
        last_id = load_checkpoint(collection_name)
        query = {"_id": {"$gt": ObjectId(last_id)}} if last_id else {}
        total_migrated = 0
        
        while True:
            # Fetch batch with retry logic
            for attempt in range(RETRY_LIMIT):
                try:
                    docs = list(src.find(query).sort("_id", 1).limit(BATCH_SIZE))
                    break
                except Exception as e:
                    if attempt < RETRY_LIMIT - 1:
                        logger.warning(f"Fetch attempt {attempt+1} failed: {e}. Retrying...")
                        time.sleep(RETRY_DELAY * (attempt + 1))
                    else:
                        logger.error(f"Failed to fetch documents after {RETRY_LIMIT} attempts: {e}")
                        raise
            
            if not docs:
                logger.info(f"[DONE] {collection_name} - Total migrated: {total_migrated}")
                break
            
            # Write batch with retry logic
            for attempt in range(RETRY_LIMIT):
                try:
                    operations = [ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for doc in docs]
                    result = dest.bulk_write(operations, ordered=False)
                    break
                except Exception as e:
                    if attempt < RETRY_LIMIT - 1:
                        logger.warning(f"Write attempt {attempt+1} failed: {e}. Retrying...")
                        time.sleep(RETRY_DELAY * (attempt + 1))
                    else:
                        logger.error(f"Failed to write documents after {RETRY_LIMIT} attempts: {e}")
                        raise
            
            # Verify the batch if needed
            verify_batch(docs, dest, collection_name)
            
            # Update progress
            last_id = docs[-1]["_id"]
            total_migrated += len(docs)
            save_checkpoint(collection_name, last_id, total_migrated)
            # Update query for next batch
            query = {"_id": {"$gt": last_id}}
            
            logger.info(f"[PROGRESS] {collection_name}: Migrated {total_migrated} documents, up to ID {last_id}")
    
    except Exception as e:
        logger.error(f"Migration failed for {collection_name}: {e}", exc_info=True)
        raise