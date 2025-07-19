import os
import sys
import json

# Required directories
DIRECTORIES = ["progress", "logs", "verification"]


def setup_environment():
    """Setup the environment for MongoDB migration"""
    print("Setting up MongoDB migration environment...")

    # Create required directories
    for directory in DIRECTORIES:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ Created directory: {directory}")

    # Check environment variables
    source_uri = os.getenv("SOURCE_URI")
    target_uri = os.getenv("TARGET_URI")

    if not source_uri:
        print("⚠ SOURCE_URI environment variable not set")
        print(
            '  Set it with: export SOURCE_URI="mongodb://user:password@source-host:27017/"'
        )
    else:
        print("✓ SOURCE_URI environment variable is set")

    if not target_uri:
        print("⚠ TARGET_URI environment variable not set")
        print(
            '  Set it with: export TARGET_URI="mongodb://user:password@target-host:27017/"'
        )
    else:
        print("✓ TARGET_URI environment variable is set")

    # Check collections.json
    try:
        with open("collections.json") as f:
            collections = json.load(f)
            print(f"✓ Found {len(collections)} collections in collections.json")
    except FileNotFoundError:
        print("⚠ collections.json not found")
        print("  Create it with your collection mapping")
    except json.JSONDecodeError:
        print("⚠ collections.json is not valid JSON")

    print("\nSetup complete!")

    if not source_uri or not target_uri:
        print(
            "\nPlease set the required environment variables before running the migration."
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(setup_environment())
