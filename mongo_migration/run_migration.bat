@echo off
REM Windows batch script to run the migration

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Run the migration with environment variables
call set_env.bat migrate