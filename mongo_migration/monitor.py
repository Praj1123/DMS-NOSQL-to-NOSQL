import json
import os
import time
import datetime
import sys
import argparse
import threading
from pymongo import MongoClient
from config.config import *

# Setup logger
logger = setup_logging("monitor")

def get_checkpoint_status():
    """Get status from checkpoint files"""
    status = {}
    
    if not os.path.exists(PROGRESS_DIR):
        return status
    
    # Load checkpoint history if it exists
    history_path = os.path.join(PROGRESS_DIR, "checkpoint_history.json")
    checkpoint_history = {}
    if os.path.exists(history_path):
        try:
            with open(history_path) as f:
                checkpoint_history = json.load(f)
        except Exception as e:
            logger.error(f"Error reading checkpoint history: {e}")
    
    for filename in os.listdir(PROGRESS_DIR):
        if not filename.endswith('.json') or filename == "checkpoint_history.json":
            continue
            
        try:
            filepath = os.path.join(PROGRESS_DIR, filename)
            with open(filepath) as f:
                data = json.load(f)
            
            collection_name = filename.replace('_cdc.json', '').replace('.json', '')
            timestamp = data.get('timestamp', 0)
            
            if isinstance(timestamp, str):
                try:
                    dt = datetime.datetime.fromisoformat(timestamp)
                    timestamp = dt.timestamp()
                except:
                    timestamp = 0
            
            # Get checkpoint history for this collection
            collection_history = checkpoint_history.get(collection_name, [])
            
            status[collection_name] = {
                'last_updated': timestamp,
                'age_seconds': time.time() - timestamp if timestamp else None,
                'checkpoint_data': data,
                'history': collection_history
            }
        except Exception as e:
            logger.error(f"Error reading checkpoint {filename}: {e}")
    
    return status

def get_collection_progress(src_client, tgt_client, source_db, target_db, collection_name):
    """Get migration progress for a collection"""
    try:
        src_count = src_client[source_db][collection_name].count_documents({})
        tgt_count = tgt_client[target_db][collection_name].count_documents({})
        
        if src_count == 0:
            progress = 100.0
        else:
            progress = (tgt_count / src_count) * 100.0
        
        # Get update and deletion stats from checkpoint if available
        updates = 0
        deletions = 0
        checkpoint_path = os.path.join(PROGRESS_DIR, f"{collection_name}_cdc.json")
        if os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path) as f:
                    checkpoint_data = json.load(f)
                    updates = checkpoint_data.get("updates", 0)
                    deletions = checkpoint_data.get("deletions", 0)
            except Exception:
                pass
            
        return {
            "source_count": src_count,
            "target_count": tgt_count,
            "progress_pct": progress,
            "remaining": src_count - tgt_count,
            "updates": updates,
            "deletions": deletions
        }
    except Exception as e:
        logger.error(f"Error getting progress for {collection_name}: {e}")
        return {
            "error": str(e)
        }

def calculate_eta(checkpoints, collection_name):
    """Calculate estimated time to completion based on checkpoint history"""
    try:
        # Need at least 2 checkpoints to calculate rate
        if len(checkpoints) < 2:
            return None
            
        # Sort by timestamp
        sorted_points = sorted(checkpoints, key=lambda x: x.get('timestamp', 0))
        
        # Get first and last checkpoint
        first = sorted_points[0]
        last = sorted_points[-1]
        
        # Calculate rate (docs per second)
        time_diff = last.get('timestamp', 0) - first.get('timestamp', 0)
        count_diff = last.get('count', 0) - first.get('count', 0)
        
        if time_diff <= 0 or count_diff <= 0:
            return None
            
        rate = count_diff / time_diff
        
        # Get current progress
        current_count = last.get('count', 0)
        
        # Get total count from MongoDB
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        collection_info = next((c for c in load_collections() if c['collection'] == collection_name), None)
        
        if not collection_info:
            return None
            
        total_count = src_client[collection_info['source_db']][collection_name].count_documents({})
        remaining = total_count - current_count
        
        if rate <= 0:
            return None
            
        # Calculate ETA in seconds
        eta_seconds = remaining / rate
        
        return {
            "rate_docs_per_sec": rate,
            "remaining_docs": remaining,
            "eta_seconds": eta_seconds,
            "eta_human": str(datetime.timedelta(seconds=int(eta_seconds)))
        }
    except Exception as e:
        logger.error(f"Error calculating ETA for {collection_name}: {e}")
        return None

def load_collections():
    """Load collections configuration"""
    try:
        with open("collections.json") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load collections: {e}")
        return []

def monitor_migration():
    """Monitor migration progress"""
    logger.info("=== MongoDB Migration Monitor Starting ===")
    
    try:
        # Load collections
        collections = load_collections()
        if not collections:
            logger.error("No collections found to monitor")
            return 1
        
        # Connect to databases
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)
        
        # Get checkpoint status
        checkpoint_status = get_checkpoint_status()
        
        # Get progress for each collection
        results = []
        for c in collections:
            collection_name = c["collection"]
            progress = get_collection_progress(
                src_client, 
                tgt_client, 
                c["source_db"], 
                c["target_db"], 
                collection_name
            )
            
            # Add checkpoint info
            checkpoint = checkpoint_status.get(collection_name, {})
            
            # Calculate ETA if we have checkpoint history
            eta = None
            if 'checkpoint_data' in checkpoint and 'count' in checkpoint['checkpoint_data'] and 'history' in checkpoint:
                history = checkpoint['history']
                if len(history) >= 2:
                    eta = calculate_eta(history, collection_name)
            
            results.append({
                "collection": collection_name,
                "source_db": c["source_db"],
                "target_db": c["target_db"],
                "progress": progress,
                "checkpoint": checkpoint,
                "eta": eta
            })
        
        # Print summary
        print("\n=== Migration Progress Summary ===")
        print(f"{'Collection':<20} {'Progress':<10} {'Source':<10} {'Target':<10} {'Updates':<10} {'Deletions':<10} {'Last Update':<20}")
        print("-" * 100)
        
        for r in results:
            progress = r["progress"]
            if "error" in progress:
                progress_str = "ERROR"
                src_count = "?"
                tgt_count = "?"
                updates = "?"
                deletions = "?"
            else:
                progress_str = f"{progress['progress_pct']:.1f}%"
                src_count = str(progress['source_count'])
                tgt_count = str(progress['target_count'])
                updates = str(progress.get('updates', 0))
                deletions = str(progress.get('deletions', 0))
            
            checkpoint = r["checkpoint"]
            if checkpoint and checkpoint.get('last_updated'):
                last_update = datetime.datetime.fromtimestamp(checkpoint['last_updated']).strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_update = "Never"
                
            print(f"{r['collection']:<20} {progress_str:<10} {src_count:<10} {tgt_count:<10} {updates:<10} {deletions:<10} {last_update:<20}")
        
        # Update checkpoint history
        history_path = os.path.join(PROGRESS_DIR, "checkpoint_history.json")
        checkpoint_history = {}
        if os.path.exists(history_path):
            try:
                with open(history_path) as f:
                    checkpoint_history = json.load(f)
            except Exception:
                checkpoint_history = {}
        
        # Update history with current checkpoint data
        for r in results:
            collection_name = r["collection"]
            if 'checkpoint_data' in r.get('checkpoint', {}):
                checkpoint_data = r['checkpoint']['checkpoint_data']
                if 'count' in checkpoint_data and 'timestamp' in checkpoint_data:
                    if collection_name not in checkpoint_history:
                        checkpoint_history[collection_name] = []
                    
                    # Add to history, keeping only the last 10 checkpoints
                    checkpoint_history[collection_name].append({
                        'timestamp': checkpoint_data['timestamp'],
                        'count': checkpoint_data['count']
                    })
                    
                    # Keep only the last 10 checkpoints
                    checkpoint_history[collection_name] = checkpoint_history[collection_name][-10:]
        
        # Save updated history
        try:
            with open(history_path, "w") as f:
                json.dump(checkpoint_history, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save checkpoint history: {e}")
        
        # Save monitoring results to a fixed file
        os.makedirs(LOG_DIR, exist_ok=True)
        output_file = os.path.join(LOG_DIR, "monitor_current.json")
        
        # Write to the current monitoring file
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Monitoring results saved to {output_file}")
        return 0
        
    except Exception as e:
        logger.error(f"Monitoring failed: {e}", exc_info=True)
        return 1

def live_monitor(refresh_rate=MONITOR_REFRESH_RATE if hasattr(sys.modules['config.config'], 'MONITOR_REFRESH_RATE') else 2):
    """Run monitoring in a continuous loop with real-time updates"""
    logger.info("=== MongoDB Migration Live Monitor Starting ===")
    print("\033[2J")  # Clear screen
    
    try:
        # Load collections
        collections = load_collections()
        if not collections:
            logger.error("No collections found to monitor")
            return 1
        
        # Connect to databases
        src_client = MongoClient(SOURCE_URI, **MONGO_CLIENT_OPTIONS)
        tgt_client = MongoClient(TARGET_URI, **MONGO_CLIENT_OPTIONS)
        
        # Create the monitor_current.json file once
        os.makedirs(LOG_DIR, exist_ok=True)
        output_file = os.path.join(LOG_DIR, "monitor_current.json")
        
        while True:
            print("\033[H")  # Move cursor to home position
            print("=== MongoDB Migration Live Monitor ===\n")
            print(f"Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("Press Ctrl+C to exit\n")
            
            # Get checkpoint status
            checkpoint_status = get_checkpoint_status()
            
            # Get progress for each collection
            results = []
            for c in collections:
                collection_name = c["collection"]
                progress = get_collection_progress(
                    src_client, 
                    tgt_client, 
                    c["source_db"], 
                    c["target_db"], 
                    collection_name
                )
                
                # Add checkpoint info
                checkpoint = checkpoint_status.get(collection_name, {})
                
                # Calculate ETA if we have checkpoint history
                eta = None
                if 'checkpoint_data' in checkpoint and 'count' in checkpoint['checkpoint_data'] and 'history' in checkpoint:
                    history = checkpoint['history']
                    if len(history) >= 2:
                        eta = calculate_eta(history, collection_name)
                
                results.append({
                    "collection": collection_name,
                    "source_db": c["source_db"],
                    "target_db": c["target_db"],
                    "progress": progress,
                    "checkpoint": checkpoint,
                    "eta": eta
                })
            
            # Print summary
            print("\n=== Migration Progress Summary ===")
            print(f"{'Collection':<20} {'Progress':<10} {'Source':<10} {'Target':<10} {'Updates':<10} {'Deletions':<10} {'Last Update':<20}")
            print("-" * 100)
            
            for r in results:
                progress = r["progress"]
                if "error" in progress:
                    progress_str = "ERROR"
                    src_count = "?"
                    tgt_count = "?"
                    updates = "?"
                    deletions = "?"
                else:
                    progress_str = f"{progress['progress_pct']:.1f}%"
                    src_count = str(progress['source_count'])
                    tgt_count = str(progress['target_count'])
                    updates = str(progress.get('updates', 0))
                    deletions = str(progress.get('deletions', 0))
                
                checkpoint = r["checkpoint"]
                if checkpoint and checkpoint.get('last_updated'):
                    last_update = datetime.datetime.fromtimestamp(checkpoint['last_updated']).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    last_update = "Never"
                    
                print(f"{r['collection']:<20} {progress_str:<10} {src_count:<10} {tgt_count:<10} {updates:<10} {deletions:<10} {last_update:<20}")
            
            # Update checkpoint history
            history_path = os.path.join(PROGRESS_DIR, "checkpoint_history.json")
            checkpoint_history = {}
            if os.path.exists(history_path):
                try:
                    with open(history_path) as f:
                        checkpoint_history = json.load(f)
                except Exception:
                    checkpoint_history = {}
            
            # Update history with current checkpoint data
            for r in results:
                collection_name = r["collection"]
                if 'checkpoint_data' in r.get('checkpoint', {}):
                    checkpoint_data = r['checkpoint']['checkpoint_data']
                    if 'count' in checkpoint_data and 'timestamp' in checkpoint_data:
                        if collection_name not in checkpoint_history:
                            checkpoint_history[collection_name] = []
                        
                        # Add to history, keeping only the last 10 checkpoints
                        checkpoint_history[collection_name].append({
                            'timestamp': checkpoint_data['timestamp'],
                            'count': checkpoint_data['count']
                        })
                        
                        # Keep only the last 10 checkpoints
                        checkpoint_history[collection_name] = checkpoint_history[collection_name][-10:]
            
            # Save updated history
            try:
                with open(history_path, "w") as f:
                    json.dump(checkpoint_history, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to save checkpoint history: {e}")
            
            # Update the monitoring file
            with open(output_file, "w") as f:
                json.dump(results, f, indent=2)
            
            # Wait for next refresh
            time.sleep(refresh_rate)
    except KeyboardInterrupt:
        print("\nLive monitoring stopped")
        return 0
    except Exception as e:
        logger.error(f"Live monitoring error: {e}", exc_info=True)
        return 1

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="MongoDB Migration Monitor")
    parser.add_argument("--live", action="store_true", help="Run in live monitoring mode")
    parser.add_argument("--refresh", type=int, default=None, 
                        help="Refresh rate in seconds for live mode")
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    if args.live:
        refresh_rate = args.refresh if args.refresh is not None else \
                      (MONITOR_REFRESH_RATE if hasattr(sys.modules['config.config'], 'MONITOR_REFRESH_RATE') else 2)
        sys.exit(live_monitor(refresh_rate))
    else:
        sys.exit(monitor_migration())