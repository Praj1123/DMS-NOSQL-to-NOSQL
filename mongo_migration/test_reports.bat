@echo off
REM Test the report generator

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Run the report test script
python test_reports.py

REM Open the reports directory
start reports\

pause