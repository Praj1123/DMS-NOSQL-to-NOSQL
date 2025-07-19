import os
import json
import time
import datetime
import sys
from pymongo import MongoClient
from config.config import *

# Setup logger
logger = setup_logging("cdc_report")

def load_collections():
    """Load collections configuration"""
    try:
        with open("collections.json") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load collections: {e}")
        return []

def get_checkpoint_data():
    """Get data from all CDC checkpoint files"""
    checkpoint_data = {}
    
    if not os.path.exists(PROGRESS_DIR):
        return checkpoint_data
    
    for filename in os.listdir(PROGRESS_DIR):
        if not filename.endswith('_cdc.json'):
            continue
            
        try:
            filepath = os.path.join(PROGRESS_DIR, filename)
            with open(filepath) as f:
                data = json.load(f)
            
            collection_name = filename.replace('_cdc.json', '')
            checkpoint_data[collection_name] = data
        except Exception as e:
            logger.error(f"Error reading checkpoint {filename}: {e}")
    
    return checkpoint_data

def generate_cdc_report():
    """Generate CDC report with update and deletion tracking"""
    logger.info("Generating CDC report with update and deletion tracking...")
    return _generate_cdc_report()

def _generate_cdc_report():
    """Internal implementation of CDC report generation"""
    logger.info("Processing CDC report data...")
    
    try:
        # Load collections
        collections = load_collections()
        if not collections:
            logger.error("No collections found")
            return 1
        
        # Connect to databases
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)
        
        # Get checkpoint data
        checkpoint_data = get_checkpoint_data()
        
        # Prepare report data
        report_data = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_collections": len(collections),
            "total_updates": 0,
            "total_deletions": 0,
            "collections": []
        }
        
        # Process each collection
        for c in collections:
            collection_name = c["collection"]
            source_db = c["source_db"]
            target_db = c["target_db"]
            
            # Get document counts
            try:
                src_count = src_client[source_db][collection_name].count_documents({})
                tgt_count = tgt_client[target_db][collection_name].count_documents({})
            except Exception as e:
                logger.error(f"Error getting counts for {collection_name}: {e}")
                src_count = 0
                tgt_count = 0
            
            # Get update and deletion counts from checkpoint
            updates = 0
            deletions = 0
            last_update = "Never"
            
            if collection_name in checkpoint_data:
                cp = checkpoint_data[collection_name]
                updates = cp.get("updates", 0)
                deletions = cp.get("deletions", 0)
                
                # Format timestamp
                if "timestamp" in cp:
                    try:
                        ts = datetime.datetime.fromisoformat(cp["timestamp"].replace('Z', '+00:00'))
                        last_update = ts.strftime("%Y-%m-%d %H:%M:%S")
                    except:
                        last_update = cp["timestamp"]
            
            # Calculate percentages for progress bars (max 100%)
            update_percent = min(100, (updates / max(1, src_count)) * 100)
            deletion_percent = min(100, (deletions / max(1, src_count)) * 100)
            
            # Add to report
            report_data["collections"].append({
                "name": collection_name,
                "source_count": src_count,
                "target_count": tgt_count,
                "updates": updates,
                "deletions": deletions,
                "update_percent": update_percent,
                "deletion_percent": deletion_percent,
                "last_update": last_update
            })
            
            # Update totals
            report_data["total_updates"] += updates
            report_data["total_deletions"] += deletions
        
        # Create reports directory if it doesn't exist
        os.makedirs("reports", exist_ok=True)
        
        # Save report as JSON
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        json_path = os.path.join("reports", f"cdc_report_{timestamp}.json")
        
        with open(json_path, "w") as f:
            json.dump(report_data, f, indent=2)
        
        # Generate HTML report
        try:
            import pystache
            with open(os.path.join("templates", "cdc_report_template.html")) as f:
                template = f.read()
            
            html = pystache.render(template, report_data)
            html_path = os.path.join("reports", f"cdc_report_{timestamp}.html")
            
            with open(html_path, "w") as f:
                f.write(html)
            
            logger.info(f"HTML report saved to {html_path}")
        except ImportError:
            logger.warning("pystache not installed, skipping HTML report generation")
            logger.info("Install with: pip install pystache")
        except Exception as e:
            logger.error(f"Error generating HTML report: {e}")
        
        logger.info(f"CDC report saved to {json_path}")
        return 0
        
    except Exception as e:
        logger.error(f"Error generating CDC report: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(_generate_cdc_report())