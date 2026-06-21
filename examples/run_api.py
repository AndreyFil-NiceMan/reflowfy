"""
Example startup script showing how to use Reflowfy.

This script:
1. Imports pipeline definitions (triggers registration)
2. Starts the API server
"""

# Import pipeline definitions - this triggers auto-registration

# Start the API
from reflowfy.api.app import main

if __name__ == "__main__":
    main()
