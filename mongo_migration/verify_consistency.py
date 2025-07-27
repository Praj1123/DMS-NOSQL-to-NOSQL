import json
import threading
from pymongo import MongoClient
from hashlib import md5
from tqdm import tqdm
from queue import Queue
import os

# CONFIG
COSMOS_URI = os.environ.get("SOURCE_URI")
EC2_URI = os.environ.get("TARGET_URI")
COLLECTION_FILE = "collections.json"
SAMPLE_LIMIT = None  # Use None for full scan or set integer for sampling

# Connect clients
cosmos_client = MongoClient(COSMOS_URI)
ec2_client = MongoClient(EC2_URI)

def hash_document(doc, exclude_keys=["_id"]):
    filtered = {k: doc[k] for k in sorted(doc) if k not in exclude_keys}
    return md5(str(filtered).encode()).hexdigest()

def compare_collection(entry, output_q):
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

    if coll_name not in source_db.list_collection_names():
        result["status"] = "Missing in Cosmos"
        output_q.put(result)
        return

    if coll_name not in target_db.list_collection_names():
        result["status"] = "Missing in EC2"
        output_q.put(result)
        return

    cosmos_cursor = source_db[coll_name].find()
    ec2_cursor = target_db[coll_name].find()

    cosmos_docs = list(cosmos_cursor.limit(SAMPLE_LIMIT)) if SAMPLE_LIMIT else list(cosmos_cursor)
    ec2_docs = list(ec2_cursor.limit(SAMPLE_LIMIT)) if SAMPLE_LIMIT else list(ec2_cursor)

    result["cosmos_count"] = len(cosmos_docs)
    result["ec2_count"] = len(ec2_docs)

    if result["cosmos_count"] != result["ec2_count"]:
        result["status"] = "Count mismatch"

    cosmos_map = {doc["_id"]: hash_document(doc) for doc in cosmos_docs if "_id" in doc}
    ec2_map = {doc["_id"]: hash_document(doc) for doc in ec2_docs if "_id" in doc}

    mismatches = 0
    for _id, h1 in cosmos_map.items():
        h2 = ec2_map.get(_id)
        if h2 is None or h1 != h2:
            mismatches += 1

    result["hash_mismatches"] = mismatches
    if mismatches > 0 and result["status"] == "OK":
        result["status"] = "Hash mismatch"

    output_q.put(result)

def main():
    with open(COLLECTION_FILE) as f:
        collection_list = json.load(f)

    output_q = Queue()
    threads = []

    print(f"\n🔍 Starting comparison for {len(collection_list)} collections...\n")

    for entry in collection_list:
        t = threading.Thread(target=compare_collection, args=(entry, output_q))
        threads.append(t)
        t.start()

    for t in tqdm(threads, desc="Processing"):
        t.join()

    results = []
    while not output_q.empty():
        results.append(output_q.get())

    print("\n=== 🔎 CONSISTENCY REPORT ===\n")
    for row in sorted(results, key=lambda x: x["collection"]):
        print(f"[{row['status']}] {row['collection']}")
        print(f"  - Cosmos Count: {row['cosmos_count']}")
        print(f"  - EC2 Count   : {row['ec2_count']}")
        print(f"  - Hash Mismatches: {row['hash_mismatches']}\n")

if __name__ == "__main__":
    main()
