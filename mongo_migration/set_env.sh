#!/bin/bash

# MongoDB connection settings
# Replace these with your actual MongoDB connection strings
export SOURCE_URI="mongodb://source_user:source1122@35.89.27.99:27017/?authSource=admin"
export TARGET_URI="mongodb://target_user:target1122@52.11.177.120:27017/?authSource=admin"

# Print confirmation
echo "Environment variables set:"
echo "SOURCE_URI=$SOURCE_URI"
echo "TARGET_URI=$TARGET_URI"

# Print multi-threading info
if [[ "$*" == *"--threads"* ]]; then
    echo "Multi-threading enabled"
    echo "To monitor in real-time, run in another terminal:"
    echo "python monitor.py --live"
fi

# Run the migration script with environment variables directly
if [ $# -gt 0 ]; then
    # Pass environment variables directly to the Python process
    SOURCE_URI="$SOURCE_URI" TARGET_URI="$TARGET_URI" python migrate.py "$@"
else
    echo "Usage: bash set_env.sh [migrate|verify|cdc|update] [options]"
    echo "  migrate - Run one-time migration of collections"
    echo "  verify  - Verify migration results"
    echo "  cdc     - Run continuous change data capture with real-time monitoring"
    echo "  update  - Update changed documents in target database"
    echo ""
    echo "Options:"
    echo "  --threads auto|N  - Use multi-threading (auto=one per collection, N=number of threads)"
    echo "  --batch-size N    - Set batch size for document processing"
    echo "  --force-refresh   - Force refresh ignoring checkpoints"
    echo ""
    echo "Examples:"
    echo "  bash set_env.sh migrate"
    echo "  bash set_env.sh cdc --threads auto"
    echo "  bash set_env.sh cdc --threads 4 --batch-size 2000"
    echo "  bash set_env.sh update"
fi