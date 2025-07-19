@echo off
REM MongoDB connection settings
REM Replace these with your actual MongoDB connection strings
set SOURCE_URI=mongodb://source_user:source1122@35.89.27.99:27017/?authSource=admin
set TARGET_URI=mongodb://target_user:target1122@52.11.177.120:27017/?authSource=admin

REM Print confirmation
echo Environment variables set:
echo SOURCE_URI=%SOURCE_URI%
echo TARGET_URI=%TARGET_URI%

REM Run the migration script if arguments are provided
if not "%1"=="" (
    python migrate.py %*
) else (
    echo Usage: set_env.bat [migrate^|verify^|cdc]
    echo   migrate - Run one-time migration of collections
    echo   verify  - Verify migration results
    echo   cdc     - Run continuous change data capture with real-time monitoring
    echo Example: set_env.bat migrate
    echo Example: set_env.bat cdc
)