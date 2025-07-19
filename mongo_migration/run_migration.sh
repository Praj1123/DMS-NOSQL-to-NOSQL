#!/bin/bash

# Activate virtual environment if it exists
if [ -d "venv/bin" ]; then
    source venv/bin/activate
fi

# Run the migration with environment variables
bash set_env.sh migrate