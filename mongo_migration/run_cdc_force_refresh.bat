@echo off
echo Running CDC with force-refresh to detect all updates and deletions...
python migrate.py cdc --force-refresh
pause