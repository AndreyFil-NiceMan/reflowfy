"""
Example startup script showing how to use Reflowfy.

This script:
1. Imports pipeline definitions (triggers registration)
2. Starts the API server
"""

# Import pipeline definitions - this triggers auto-registration
import xml_to_json_pipeline

# Start the API
from reflowfy.api.app import main

if __name__ == "__main__":
    main()
