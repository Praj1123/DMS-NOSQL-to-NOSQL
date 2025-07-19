import json
import os
import sys
import time
import hashlib
from pymongo import MongoClient
from config.config import *

# Setup logger
logger = setup_logging("verify")


def get_collection_stats(client, db_name, collection_name):
    """Get collection statistics"""
    try:
        db = client[db_name]
        stats = db.command("collStats", collection_name)
        return {
            "count": stats.get("count", 0),
            "size": stats.get("size", 0),
            "avgObjSize": stats.get("avgObjSize", 0),
            "storageSize": stats.get("storageSize", 0),
            "indexes": len(stats.get("indexSizes", {})),
        }
    except Exception as e:
        logger.error(f"Failed to get stats for {db_name}.{collection_name}: {e}")
        return None


def verify_indexes(src_client, tgt_client, source_db, target_db, collection_name):
    """Verify indexes match between source and target collections"""
    try:
        src_indexes = src_client[source_db][collection_name].index_information()
        tgt_indexes = tgt_client[target_db][collection_name].index_information()

        # Compare index counts
        if len(src_indexes) != len(tgt_indexes):
            logger.warning(
                f"Index count mismatch for {collection_name}: source={len(src_indexes)}, target={len(tgt_indexes)}"
            )
            return False

        # Compare each index
        for idx_name, idx_info in src_indexes.items():
            if idx_name not in tgt_indexes:
                logger.warning(
                    f"Index {idx_name} missing in target for {collection_name}"
                )
                return False

            # Compare key patterns
            src_keys = idx_info["key"]
            tgt_keys = tgt_indexes[idx_name]["key"]
            if src_keys != tgt_keys:
                logger.warning(
                    f"Index {idx_name} key pattern mismatch for {collection_name}"
                )
                return False

        return True
    except Exception as e:
        logger.error(f"Error verifying indexes for {collection_name}: {e}")
        return False


def verify_document_sample(
    src_client, tgt_client, source_db, target_db, collection_name, sample_size=100
):
    """Verify a sample of documents match between source and target"""
    try:
        src_coll = src_client[source_db][collection_name]
        tgt_coll = tgt_client[target_db][collection_name]

        # Get total count
        total_count = src_coll.count_documents({})
        if total_count == 0:
            logger.info(f"Collection {collection_name} is empty in source")
            return True

        # Determine skip interval for sampling
        skip_interval = max(1, total_count // sample_size)

        # Sample documents
        mismatches = 0
        checked = 0

        for i in range(0, min(total_count, sample_size * skip_interval), skip_interval):
            src_doc = src_coll.find_one(skip=i)
            if not src_doc:
                continue

            doc_id = src_doc["_id"]
            tgt_doc = tgt_coll.find_one({"_id": doc_id})

            if not tgt_doc:
                logger.warning(
                    f"Document {doc_id} missing in target collection {collection_name}"
                )
                mismatches += 1
                continue

            # Compare documents using hash
            src_hash = hashlib.md5(
                json.dumps(src_doc, sort_keys=True).encode()
            ).hexdigest()
            tgt_hash = hashlib.md5(
                json.dumps(tgt_doc, sort_keys=True).encode()
            ).hexdigest()

            if src_hash != tgt_hash:
                logger.warning(
                    f"Document {doc_id} content mismatch in {collection_name}"
                )
                mismatches += 1

            checked += 1

        match_percentage = 100 - (mismatches / checked * 100) if checked > 0 else 0
        logger.info(
            f"Document verification for {collection_name}: {match_percentage:.2f}% match ({mismatches} mismatches in {checked} samples)"
        )

        return match_percentage >= 99.0  # Allow 1% tolerance
    except Exception as e:
        logger.error(f"Error verifying documents for {collection_name}: {e}")
        return False


def verify_collection(src_client, tgt_client, source_db, target_db, collection_name):
    """Verify a collection is properly mirrored"""
    logger.info(f"Verifying collection: {collection_name}")
    results = {
        "collection": collection_name,
        "source_db": source_db,
        "target_db": target_db,
        "timestamp": time.time(),
        "checks": {},
    }

    # Check 1: Collection exists
    try:
        collection_exists = (
            collection_name in tgt_client[target_db].list_collection_names()
        )
        results["checks"]["exists"] = collection_exists
        if not collection_exists:
            logger.error(
                f"Collection {collection_name} does not exist in target database"
            )
            return results
    except Exception as e:
        logger.error(f"Error checking collection existence: {e}")
        results["checks"]["exists"] = False
        return results

    # Check 2: Document count
    try:
        src_count = src_client[source_db][collection_name].count_documents({})
        tgt_count = tgt_client[target_db][collection_name].count_documents({})
        count_match = abs(src_count - tgt_count) <= max(
            5, src_count * 0.01
        )  # Allow 1% or 5 docs difference

        results["checks"]["count"] = {
            "source": src_count,
            "target": tgt_count,
            "match": count_match,
        }

        if not count_match:
            logger.warning(
                f"Document count mismatch for {collection_name}: source={src_count}, target={tgt_count}"
            )
    except Exception as e:
        logger.error(f"Error checking document count: {e}")
        results["checks"]["count"] = {"error": str(e)}

    # Check 3: Indexes
    try:
        indexes_match = verify_indexes(
            src_client, tgt_client, source_db, target_db, collection_name
        )
        results["checks"]["indexes"] = indexes_match
    except Exception as e:
        logger.error(f"Error checking indexes: {e}")
        results["checks"]["indexes"] = False

    # Check 4: Document sample verification
    try:
        docs_match = verify_document_sample(
            src_client, tgt_client, source_db, target_db, collection_name
        )
        results["checks"]["documents"] = docs_match
    except Exception as e:
        logger.error(f"Error checking document samples: {e}")
        results["checks"]["documents"] = False

    # Overall status
    all_checks = [
        results["checks"]["exists"],
        (
            results["checks"]["count"]["match"]
            if isinstance(results["checks"].get("count"), dict)
            and "match" in results["checks"]["count"]
            else False
        ),
        results["checks"]["indexes"],
        results["checks"]["documents"],
    ]

    results["status"] = "OK" if all(all_checks) else "FAILED"
    logger.info(f"Verification {results['status']} for {collection_name}")

    return results


def main():
    """Main verification function"""
    logger.info("=== MongoDB Migration Verification Starting ===")

    try:
        # Load collections
        with open("collections.json") as f:
            collections = json.load(f)

        # Connect to databases
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)

        # Test connections
        src_client.admin.command("ping")
        tgt_client.admin.command("ping")

        # Verify each collection
        results = []
        for c in collections:
            result = verify_collection(
                src_client, tgt_client, c["source_db"], c["target_db"], c["collection"]
            )
            results.append(result)

        # Save verification results
        os.makedirs(VERIFICATION_DIR, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(VERIFICATION_DIR, f"verification_{timestamp}.json")

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)

        logger.info(f"Verification results saved to {output_file}")

        # Summary
        failed = [r["collection"] for r in results if r["status"] != "OK"]
        if failed:
            logger.warning(
                f"Verification failed for {len(failed)} collections: {', '.join(failed)}"
            )
            return 1
        else:
            logger.info(f"All {len(results)} collections verified successfully")
            return 0

    except Exception as e:
        logger.error(f"Verification failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
