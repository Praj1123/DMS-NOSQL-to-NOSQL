# MongoDB Migration Script

A robust, reliable Python-based MongoDB data migration tool designed to create perfect mirrors of source databases with comprehensive error handling, verification, and monitoring.

## Features

- **Reliable Migration**: Comprehensive error handling and retry mechanisms
- **Data Verification**: Ensures destination is an exact mirror of source
- **Checkpointing**: Resume migrations from where they left off
- **Parallel Processing**: Concurrent migration of multiple collections
- **Change Data Capture (CDC)**: Continuous synchronization after initial migration
- **Monitoring**: Track migration progress and performance
- **Graceful Shutdown**: Safely stop migrations without data corruption

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   ```
   export SOURCE_URI="mongodb://user:password@source-host:27017/"
   export TARGET_URI="mongodb://user:password@target-host:27017/"
   ```

3. Edit `collections.json` to specify which collections to migrate:
   ```json
   [
     { "source_db": "source_database", "target_db": "target_database", "collection": "collection_name" }
   ]
   ```

## Usage

### Using the Environment Setup Script

The project includes a convenient shell script (`set_env.sh` for Linux/Mac or `set_env.bat` for Windows) that sets up the required environment variables and runs the migration tool:

```bash
# Linux/Mac
bash set_env.sh [migrate|verify|cdc|update]

# Windows
set_env.bat [migrate|verify|cdc|update]
```

Examples:
```bash
# Run initial migration
bash set_env.sh migrate

# Run update mode
bash set_env.sh update
```

### Manual Execution

#### Initial Migration

```
python migrate.py migrate
```

#### CDC Mode (Continuous Synchronization)

```
python migrate.py cdc
```

The CDC mode supports two approaches:

1. **Change Streams** (for MongoDB replica sets): Real-time monitoring using MongoDB change streams
2. **Polling** (fallback for standalone servers): Periodic polling for changes based on timestamps

#### Multi-threaded CDC Mode

For improved performance, you can run CDC with multiple threads (one per collection):

```bash
# Run CDC with auto-threading (one thread per collection)
bash set_env.sh cdc --threads auto

# Run CDC with a specific number of threads
bash set_env.sh cdc --threads 4 --batch-size 2000
```

Options for the `--threads` parameter:
- `auto`: Automatically use one thread per collection
- `<number>`: Specify exact number of threads to use (e.g., `--threads 4`)

This creates a separate worker thread for each collection, allowing parallel processing of changes across all collections simultaneously. Each thread continuously monitors its assigned collection and synchronizes changes in real-time.

### Update Mode (One-time Sync of Changed Documents)

```
python migrate.py update
```

This mode performs a one-time update of all documents that have changed in the source database. It's useful for syncing changes when CDC mode isn't running.

### Verification Only

```
python migrate.py verify
```

Alternatively, use the dedicated verification script:

```
python verify.py
```

### Real-time Monitoring

The tool provides real-time monitoring capabilities to track migration progress:

#### Basic Monitoring

To check current migration progress and status:

```bash
python monitor.py
```

This displays a summary of all collections being migrated, including:
- Progress percentage for each collection
- Source and target document counts
- Update and deletion counts
- Last update timestamp
- Estimated time to completion (ETA)

The monitoring results are saved to two files:
- `logs/monitor_current.json`: Always contains the most recent monitoring data
- `logs/monitor_YYYYMMDD_HHMMSS.json`: Timestamped snapshots for historical reference

#### Continuous Monitoring in CDC Mode

When running in CDC (Change Data Capture) mode, the tool automatically provides real-time monitoring of ongoing changes:

```bash
bash set_env.sh cdc
```

The CDC mode continuously tracks changes in the source database and applies them to the target database in real-time. It also tracks updates and deletions with percentage indicators:

```
=== Migration Progress Summary ===
Collection          Progress   Source     Target     Updates    Deletions  Last Update         
-------------------------------------------------------------------------------------------------
users               100.0%     1000       1000       25         5          2025-07-19 15:33:53
orders              98.5%      2000       1970       120        15         2025-07-19 15:35:12
agents              100.0%     500        500        10         0          2025-07-19 15:30:45
```

This shows:
- Collection name
- Overall progress percentage
- Source and target document counts
- Number of updates processed
- Number of deletions processed
- Last update timestamp

#### Real-time Multi-threaded Monitoring

For advanced real-time monitoring with multi-threading:

```bash
# Terminal 1: Run CDC with multi-threading (one thread per collection)
bash set_env.sh cdc --threads auto

# Terminal 2: Monitor all threads in real-time with 1-second refresh rate
python monitor.py --live --refresh 1
```

This setup provides:
- Parallel processing with one thread per collection
- Real-time comparison between source and target databases
- Immediate synchronization of any detected changes
- Live monitoring dashboard showing all collection statuses simultaneously
- Continuous updates with configurable refresh rate

### Report Generation

The tool provides comprehensive HTML reports for migration, verification, and update operations. To generate all reports at once:

```bash
# Linux/Mac
bash generate_all_reports.sh

# Windows
generate_all_reports.bat
```

Alternatively, you can run the Python script directly:

```
python generate_all_reports.py
```

Reports are saved in the `reports/` directory and include:
- Migration reports showing successful and failed collections
- Verification reports comparing source and target databases
- Update reports showing changed documents
- CDC reports showing update and deletion statistics with progress bars
- A dashboard that combines all reports in one view

CDC reports are automatically generated after running CDC mode and provide visual progress bars for updates and deletions.

## Usage Scenarios

### Complete Migration Workflow Example

Here's a step-by-step scenario for migrating a production MongoDB database to a new server:

#### 1. Configure Connection Strings

Edit `set_env.sh` (Linux/Mac) or `set_env.bat` (Windows) to set your MongoDB connection strings:

```bash
export SOURCE_URI="mongodb://source_user:password@source-server:27017/?authSource=admin"
export TARGET_URI="mongodb://target_user:password@target-server:27017/?authSource=admin"
```

#### 2. Configure Collections to Migrate

Edit `collections.json` to specify which collections to migrate:

```json
[
  { "source_db": "customers", "target_db": "customers", "collection": "users" },
  { "source_db": "customers", "target_db": "customers", "collection": "orders" },
  { "source_db": "inventory", "target_db": "inventory", "collection": "products" }
]
```

#### 3. Run Initial Migration

```bash
# On Linux/Mac
bash set_env.sh migrate

# On Windows
set_env.bat migrate
```

#### 4. Monitor Migration Progress

```bash
python monitor.py
```

#### 5. Verify Migration Results

```bash
bash set_env.sh verify
```

#### 6. Generate Reports

```bash
# On Linux/Mac
bash generate_all_reports.sh

# On Windows
generate_all_reports.bat
```

#### 7. Start CDC Mode for Continuous Synchronization

```bash
# Option 1: Standard CDC mode
bash set_env.sh cdc

# Option 2: Multi-threaded CDC mode for better performance
bash set_env.sh cdc --threads auto

# Option 3: Start real-time monitoring in a separate terminal
python monitor.py --live
```

#### 8. Final Cutover

When ready to switch to the new database:

1. Stop applications writing to the source database
2. Wait for CDC to finish processing any remaining changes
3. Run a final verification
   ```bash
   bash set_env.sh verify
   ```
4. Point your applications to the new target database

## Advanced Configuration

### Multi-threaded CDC Configuration

To configure multi-threaded CDC mode, you can edit the `config/config.py` file:

```python
# Multi-threading settings
MAX_THREADS = 0  # 0 means one thread per collection, or specify a number
THREAD_BATCH_SIZE = 1000  # Documents to process in each batch
POLLING_INTERVAL = 5  # Seconds between polling for changes

# Real-time monitoring settings
MONITOR_REFRESH_RATE = 2  # Seconds between monitor updates in live mode
```

### Command-line Options

The tool supports various command-line options for fine-tuning the migration process:

```
Usage: bash set_env.sh [migrate|verify|cdc|update] [options]

Options:
  --threads auto|N  - Use multi-threading (auto=one per collection, N=number of threads)
  --batch-size N    - Set batch size for document processing
  --force-refresh   - Force refresh ignoring checkpoints
```

### Example Commands

```bash
# Run initial migration
bash set_env.sh migrate

# Run CDC with auto-threading (one thread per collection)
bash set_env.sh cdc --threads auto

# Run CDC with 4 threads and larger batch size
bash set_env.sh cdc --threads 4 --batch-size 2000

# Run CDC with forced refresh (ignore checkpoints)
bash set_env.sh cdc --threads auto --force-refresh

# Run CDC with forced refresh only - best for detecting all updates and deletions
bash set_env.sh cdc --force-refresh

# Use the convenience script for force-refresh with update/deletion tracking
run_cdc_force_refresh.bat
```

## Update and Deletion Tracking

The tool provides comprehensive tracking of updates and deletions with visual progress indicators:

- **Update Tracking**: Counts and displays the number of documents updated during CDC operations
- **Deletion Tracking**: Detects and counts documents that have been deleted from the source database
- **Visual Progress Bars**: CDC reports include progress bars for both updates and deletions
- **Percentage Indicators**: Monitor output shows update and deletion counts with percentage completion

To maximize update and deletion detection, use the force-refresh option:

```bash
# Run CDC with force-refresh for thorough update and deletion detection
bash set_env.sh cdc --force-refresh
```

This mode will:
1. Ignore existing checkpoints to scan all documents
2. Use a larger sample size for deletion detection (500 documents)
3. Perform targeted document checks to find updates
4. Generate a comprehensive CDC report with update and deletion statistics

### Real-time Update and Deletion Tracking

The tool now includes integrated real-time update and deletion tracking in all modes:

- **Automatic Detection**: The system automatically detects when target has more documents than source
- **Proactive Deletion Handling**: Deletions are detected and applied in real-time
- **Update Tracking**: Document updates are tracked and counted
- **Threaded Mode Support**: All features work seamlessly in both single-threaded and multi-threaded modes

No special commands or separate functionality is needed - the system automatically handles updates and deletions in all modes:

## Data Consistency Guarantees

### CDC Update Detection Mechanisms

The tool uses multiple methods to detect document changes:

1. **Timestamp-based Detection**: Documents with newer `updatedAt` timestamps are automatically synchronized

2. **Content Hash Comparison**: For documents without timestamps, the tool compares content hashes to detect changes

3. **Periodic Full Scans**: Configurable full collection scans catch any changes that might be missed

To ensure all updates are detected:

```bash
# Run CDC with more aggressive polling
bash set_env.sh cdc --polling-interval 2

# Force a full refresh periodically
bash set_env.sh update --force-refresh
```

### Mirror Image Verification

The tool provides 99.9%+ data consistency guarantees through multiple mechanisms:

1. **Document-level Verification**: Every document is verified using MD5 hashing to ensure exact content matching between source and target

2. **Collection-level Verification**: The verification process compares document counts and samples documents to ensure collections match

3. **Automatic Retry Logic**: Failed operations are automatically retried with exponential backoff

4. **Checkpointing**: Progress is tracked at the document level to ensure no data is lost during interruptions

5. **CDC Consistency**: Change Data Capture ensures ongoing changes are replicated with minimal latency

## Best Practices

1. **Test First**: Always test on a small subset of data before full migration
2. **Verify**: Run verification after migration to ensure data integrity
3. **Monitor**: Keep track of migration progress using the monitoring tool
4. **Indexes**: The tool automatically copies indexes, but verify they're created correctly
5. **Backup**: Always have backups before starting large migrations
6. **Force Refresh**: Use force-refresh periodically to ensure all updates and deletions are detected
7. **Threaded Mode**: For large collections, use multi-threading but monitor system resources
8. **Graceful Shutdown**: Use Ctrl+C to gracefully stop the migration process, allowing in-progress operations to complete

## Troubleshooting

- Check logs in the `logs/` directory for detailed error information
- Verification results are stored in the `verification/` directory
- Failed documents are logged in `logs/{collection_name}_failed_docs.log`
- If migration fails, it can be safely restarted and will continue from the last checkpoint
- If updates or deletions are detected but not applied, try running with `--force-refresh` flag