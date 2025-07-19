@echo off
REM Generate all HTML reports

REM Activate virtual environment if it exists
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Run the report generator script
python generate_all_reports.py

REM Open the reports directory
start reports\

pause