import os
import logging
from logging.handlers import RotatingFileHandler
import sys

# Database configuration
SOURCE_URI = os.getenv("SOURCE_URI")
TARGET_URI = os.getenv("TARGET_URI")

# Fallback to default if environment variables are not set
if SOURCE_URI is None:
    print("WARNING: SOURCE_URI environment variable not found!")
    SOURCE_URI = "mongodb://source_user:source1122@35.89.27.99:27017/?authSource=admin"

if TARGET_URI is None:
    print("WARNING: TARGET_URI environment variable not found!")
    TARGET_URI = "mongodb://target_user:target1122@52.11.177.120:27017/?authSource=admin"

print(f"Config using SOURCE_URI: {SOURCE_URI}")
print(f"Config using TARGET_URI: {TARGET_URI}")

# Performance settings
BATCH_SIZE = 1000
CONCURRENCY = 4

# CDC settings
CDC_FORCE_REFRESH = True  # Set to True to ignore last checkpoint and scan all documents
CDC_DEBUG = True  # Set to True to print debug information about CDC queries

# Multi-threading settings
MAX_THREADS = 0  # 0 means one thread per collection, or specify a number
THREAD_BATCH_SIZE = 1000  # Documents to process in each batch
POLLING_INTERVAL = 5  # Seconds between polling for changes
MONITOR_REFRESH_RATE = 2  # Seconds between monitor updates in live mode

# Reliability settings
RETRY_LIMIT = 5
RETRY_DELAY = 2  # seconds
CONNECTION_TIMEOUT = 60000  # ms (increased to 60 seconds)
SOCKET_TIMEOUT = 60000  # ms (increased to 60 seconds)

# Directory settings
PROGRESS_DIR = "progress"
LOG_DIR = "logs"
VERIFICATION_DIR = "verification"

# Ensure directories exist
for directory in [PROGRESS_DIR, LOG_DIR, VERIFICATION_DIR]:
    os.makedirs(directory, exist_ok=True)

# Configure logging
def setup_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(console_format)
    
    # File handler
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, f"{name}.log"),
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Add handlers
    logger.addHandler(console)
    logger.addHandler(file_handler)
    
    return logger

# MongoDB client options for reliability
MONGO_CLIENT_OPTIONS = {
    "connectTimeoutMS": CONNECTION_TIMEOUT,
    "socketTimeoutMS": SOCKET_TIMEOUT,
    "serverSelectionTimeoutMS": 60000,  # Increased to 60 seconds
    "w": "majority",  # Write concern
    "retryWrites": True,
    "retryReads": True,
    "maxPoolSize": 50,  # Increased connection pool size
    "maxIdleTimeMS": 60000,  # Connection idle timeout
    "appName": "MongoMigration"  # Helps identify this application in server logs
}