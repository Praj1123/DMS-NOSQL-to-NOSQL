#!/bin/bash
# Install required dependencies

# Activate virtual environment if it exists
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
fi

# Install requirements
pip install -r requirements.txt

echo "Dependencies installed successfully!"