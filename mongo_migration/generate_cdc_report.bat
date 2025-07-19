@echo off
echo Generating CDC Report...
python generate_cdc_report.py
if %ERRORLEVEL% EQU 0 (
    echo Report generated successfully!
    echo Check the reports directory for the latest report.
) else (
    echo Error generating report.
)
pause