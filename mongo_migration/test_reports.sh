#!/bin/bash
# Test the report generator

# Activate virtual environment if it exists
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
fi

# Ensure dependencies are installed
pip install -r requirements.txt

# Ensure reports directory exists
mkdir -p reports

# Run the report test script
python test_reports.py

# Open the reports directory if it exists
if [ -d "reports" ]; then
    if command -v xdg-open > /dev/null; then
        xdg-open reports/
    elif command -v open > /dev/null; then
        open reports/
    else
        echo "Reports generated in reports/ directory"
    fi
else
    echo "Warning: reports directory was not created"
fi

# Wait for user input before closing
read -p "Press Enter to continue..."