# main.py
# Command-line interface for the Crisp Data Transformer library.

import argparse
import sys
import os # Used for path validation

# Ensure the script can find the crisp_transformer package
# If main.py is in the same directory as the crisp_transformer folder,
# Python should find it automatically. If not, adjust sys.path if necessary.
try:
    from crisp_transformer.transformer import DataTransformer
    # Import specific exceptions we might want to catch explicitly
    from crisp_transformer.exceptions import CrispTransformerError, ConfigError, FileProcessingError
except ImportError as e:
    print(f"Error: Cannot import the 'crisp_transformer' library. {e}")
    print("Ensure main.py is in the correct directory relative to the 'crisp_transformer' folder.")
    sys.exit(1)

def main():
    """Parses arguments and runs the data transformation."""

    parser = argparse.ArgumentParser(
        description="Crisp Data Transformation Tool - Transforms CSV data based on a JSON configuration.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show default values in help
    )

    # Required arguments
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the JSON configuration file."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV file."
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write the transformed output CSV file."
    )

    # Optional arguments
    parser.add_argument(
        "--errors",
        help="Optional path to write rows that failed transformation."
    )
    parser.add_argument(
        "--progress",
        type=int,
        default=1000,
        help="Log progress every N rows. Set to 0 to disable."
    )

    args = parser.parse_args()

    # Basic input validation
    if not os.path.exists(args.config):
        print(f"Error: Configuration file not found at '{args.config}'")
        sys.exit(1)
    if not os.path.exists(args.input):
        print(f"Error: Input CSV file not found at '{args.input}'")
        sys.exit(1)

    try:
        # 1. Initialize the transformer (loads and validates config)
        transformer = DataTransformer(config_path=args.config)

        # 2. Run the processing
        transformer.process(
            input_path=args.input,
            output_path=args.output,
            errors_path=args.errors,
            progress_interval=args.progress
        )

        print("Processing completed successfully.")

    # Catch specific errors from our library first
    except FileNotFoundError as e: # Should be caught by os.path check or inside process
         print(f"\nERROR: File not found - {e}")
         sys.exit(1)
    except ConfigError as e:
        print(f"\nERROR in configuration: {e}")
        sys.exit(1)
    except FileProcessingError as e:
        print(f"\nERROR processing file: {e}")
        sys.exit(1)
    except CrispTransformerError as e: # Catch any other library-specific errors
        print(f"\nERROR during transformation: {e}")
        sys.exit(1)
    # Catch unexpected errors
    except Exception as e:
        print(f"\nUNEXPECTED ERROR occurred: {e}")
        # Consider logging the full traceback here for debugging
        # import traceback
        # traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
