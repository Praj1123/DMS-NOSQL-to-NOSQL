import json
import os
import threading
import time
from pymongo import MongoClient, errors
from hashlib import md5
from tqdm import tqdm
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIG
COSMOS_URI = os.environ.get("SOURCE_URI")
EC2_URI = os.environ.get("TARGET_URI")
COLLECTION_FILE = "collections.json"
SAMPLE_LIMIT = None  # Use None for full scan or set integer for sampling
MAX_WORKERS = 5      # Limit concurrent threads
RETRY_ATTEMPTS = 3   # Retry reads on AutoReconnect
RETRY_DELAY = 2      # Delay (in seconds) between retries

# Connect clients
cosmos_client = MongoClient(COSMOS_URI)
ec2_client = MongoClient(EC2_URI)

def hash_document(doc, exclude_keys=["_id"]):
    filtered = {k: doc[k] for k in sorted(doc) if k not in exclude_keys}
    return md5(str(filtered).encode()).hexdigest()

def safe_find(collection, sample_limit=None):
    """Performs find() with retry logic on AutoReconnect errors."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            cursor = collection.find(batch_size=100)
            return list(cursor.limit(sample_limit)) if sample_limit else list(cursor)
        except errors.AutoReconnect as e:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise e

def compare_collection(entry):
    source_db = cosmos_client[entry["source_db"]]
    target_db = ec2_client[entry["target_db"]]
    coll_name = entry["collection"]

    result = {
        "collection": coll_name,
        "status": "OK",
        "cosmos_count": 0,
        "ec2_count": 0,
        "hash_mismatches": 0
    }

    try:
        if coll_name not in source_db.list_collection_names():
            result["status"] = "Missing in Cosmos"
            return result

        if coll_name not in target_db.list_collection_names():
            result["status"] = "Missing in EC2"
            return result

        cosmos_docs = safe_find(source_db[coll_name], SAMPLE_LIMIT)
        ec2_docs = safe_find(target_db[coll_name], SAMPLE_LIMIT)

        result["cosmos_count"] = len(cosmos_docs)
        result["ec2_count"] = len(ec2_docs)

        if result["cosmos_count"] != result["ec2_count"]:
            result["status"] = "Count mismatch"

        cosmos_map = {doc["_id"]: hash_document(doc) for doc in cosmos_docs if "_id" in doc}
        ec2_map = {doc["_id"]: hash_document(doc) for doc in ec2_docs if "_id" in doc}

        mismatches = sum(1 for _id, h1 in cosmos_map.items() if ec2_map.get(_id) != h1)

        result["hash_mismatches"] = mismatches
        if mismatches > 0 and result["status"] == "OK":
            result["status"] = "Hash mismatch"

    except Exception as e:
        result["status"] = f"Error: {str(e)}"

    return result

def main():
    with open(COLLECTION_FILE) as f:
        collection_list = json.load(f)

    results = []

    print(f"\n🔍 Starting comparison for {len(collection_list)} collections...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entry = {executor.submit(compare_collection, entry): entry for entry in collection_list}
        for future in tqdm(as_completed(future_to_entry), total=len(collection_list), desc="Processing"):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                entry = future_to_entry[future]
                results.append({
                    "collection": entry["collection"],
                    "status": f"Error: {str(e)}",
                    "cosmos_count": 0,
                    "ec2_count": 0,
                    "hash_mismatches": 0
                })

    print("\n=== 🔎 CONSISTENCY REPORT ===\n")
    for row in sorted(results, key=lambda x: x["collection"]):
        print(f"[{row['status']}] {row['collection']}")
        print(f"  - Cosmos Count: {row['cosmos_count']}")
        print(f"  - EC2 Count   : {row['ec2_count']}")
        print(f"  - Hash Mismatches: {row['hash_mismatches']}\n")

if __name__ == "__main__":
    main()
