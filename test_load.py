# test_load.py
# Simple script to test loading the configuration.

# Make sure Python can find the crisp_transformer package.
# If test_load.py is in the same folder as the crisp_transformer directory,
# this import should work directly.
from crisp_transformer.config_loader import load_config
from crisp_transformer.exceptions import CrispTransformerError

CONFIG_FILE = "config.json" # Path relative to this script

print(f"Attempting to load configuration from: {CONFIG_FILE}")

try:
    config = load_config(CONFIG_FILE)
    print("\n--- Configuration Data ---")
    # Pretty print the loaded config
    import json
    print(json.dumps(config, indent=2))
    print("\n--------------------------")

except FileNotFoundError:
    print(f"\nERROR: Configuration file '{CONFIG_FILE}' not found.")
except CrispTransformerError as e:
    # Catch specific errors from our library
    print(f"\nERROR loading configuration: {e}")
except Exception as e:
    # Catch any other unexpected errors
    print(f"\nUNEXPECTED ERROR: {e}")