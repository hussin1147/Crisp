# crisp_transformer/transformer.py
# Contains the main DataTransformer class responsible for orchestrating the process.

import csv
import sys
from typing import List, Dict, Any, Optional, Tuple, Callable

# Import necessary components from within the package
from .config_loader import load_config
from .operations import get_operation_handler
from .exceptions import CrispTransformerError, ConfigError, OperationError, FileProcessingError

# Configure CSV handling to be robust for large fields
# Using sys maxsize might be excessive, consider a large but reasonable limit
# csv.field_size_limit(sys.maxsize)
# Let's set a large, but not unlimited, field size limit (e.g., 128MB)
# Adjust as needed based on expected data.
try:
    csv.field_size_limit(131072 * 10) # 10 * 128k buffer size
except OverflowError:
    print("Warning: Could not set CSV field size limit (platform limitation).")


class DataTransformer:
    """
    Orchestrates the data transformation process based on a configuration file.

    Reads an input CSV, applies a series of configured transformation steps,
    writes valid rows to an output CSV, and optionally logs errors to a separate CSV.
    """

    def __init__(self, config_path: str):
        """
        Initializes the DataTransformer.

        Args:
            config_path: Path to the JSON configuration file.

        Raises:
            FileNotFoundError: If the config file is not found.
            ConfigError: If the configuration is invalid.
            FileProcessingError: If the config file cannot be read.
        """
        print(f"Initializing DataTransformer with config: {config_path}")
        try:
            self.config = load_config(config_path)
            self.target_columns = self.config.get("target_columns", [])
            self.pipeline: List[Tuple[Callable, Dict[str, Any]]] = self._build_pipeline()
            print(f"Transformation pipeline built with {len(self.pipeline)} steps.")
        except CrispTransformerError as e:
            print(f"Error during transformer initialization: {e}")
            # Re-raise to prevent using an improperly configured transformer
            raise e

    def _build_pipeline(self) -> List[Tuple[Callable, Dict[str, Any]]]:
        """
        Builds the transformation pipeline based on the loaded configuration.

        Returns:
            A list of tuples, where each tuple contains a callable operation
            handler and its corresponding configuration dictionary.

        Raises:
            ConfigError: If any operation handler cannot be resolved or is invalid.
        """
        pipeline = []
        for i, step_config in enumerate(self.config.get("transformations", [])):
            try:
                handler = get_operation_handler(step_config)
                pipeline.append((handler, step_config))
            except ConfigError as e:
                # Add context about which step failed
                raise ConfigError(f"Invalid configuration for transformation step {i+1} ('{step_config.get('operation', 'N/A')}'): {e.message}", config_path=self.config.get("config_path")) from e
        return pipeline

    def process(self, input_path: str, output_path: str, errors_path: Optional[str] = None, progress_interval: int = 1000) -> None:
        """
        Processes the input CSV file according to the configured transformations.

        Args:
            input_path: Path to the input CSV file.
            output_path: Path to write the transformed output CSV file.
            errors_path: Optional path to write rows that failed transformation.
            progress_interval: Log progress every N rows. Set to 0 to disable.
        """
        print(f"Starting processing: {input_path} -> {output_path}")
        if errors_path:
            print(f"Logging errors to: {errors_path}")

        processed_count = 0
        success_count = 0
        error_count = 0
        input_fieldnames: List[str] = []
        error_details: List[Dict[str, Any]] = [] # Store errors in memory first

        try:
            # --- Setup Input CSV Reading ---
            # Use newline='' as recommended by csv module docs
            with open(input_path, mode='r', newline='', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                input_fieldnames = reader.fieldnames if reader.fieldnames else []
                if not input_fieldnames:
                     print("Warning: Input CSV appears to be empty or has no header.")
                     # Decide how to handle empty input: maybe create empty output and return?
                     # For now, let's proceed, it will just finish quickly.

                # --- Setup Output CSV Writing ---
                # Ensure target_columns are defined, otherwise use input headers?
                # The spec implies a defined target schema.
                if not self.target_columns:
                    raise ConfigError("Configuration must define 'target_columns'.")

                with open(output_path, mode='w', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=self.target_columns, extrasaction='ignore')
                    writer.writeheader()

                    # --- Process Rows ---
                    for i, row in enumerate(reader):
                        row_number = i + 1 # 1-based index for user reporting
                        processed_count += 1
                        original_row = row.copy() # Keep original for error reporting
                        current_row = row # Process in place

                        try:
                            # Apply pipeline steps sequentially
                            for handler, step_config in self.pipeline:
                                handler(current_row, step_config, row_number)

                            # If pipeline completes, write the filtered row
                            writer.writerow(current_row)
                            success_count += 1

                        except OperationError as e:
                            error_count += 1
                            # Store error details for later writing
                            error_info = {
                                "row_number": row_number,
                                "error_message": e.message,
                                "failed_column": e.column,
                                # Include original row data prefixed to avoid clashes
                                **{f"original_{k}": v for k, v in original_row.items()}
                            }
                            error_details.append(error_info)
                        except Exception as e:
                            # Catch unexpected errors during transformation
                            error_count += 1
                            print(f"Unexpected error processing row {row_number}: {e}") # Log unexpected errors
                            error_info = {
                                "row_number": row_number,
                                "error_message": f"Unexpected error: {e}",
                                "failed_column": None,
                                **{f"original_{k}": v for k, v in original_row.items()}
                            }
                            error_details.append(error_info)


                        # --- Progress Reporting ---
                        if progress_interval > 0 and row_number % progress_interval == 0:
                            print(f"Processed {row_number} rows...")

            # --- Write Errors (if path provided) ---
            if errors_path and error_details:
                print(f"Writing {error_count} errors to {errors_path}...")
                # Define error file headers dynamically
                error_fieldnames = ["row_number", "error_message", "failed_column"] + \
                                   [f"original_{k}" for k in input_fieldnames]
                try:
                    with open(errors_path, mode='w', newline='', encoding='utf-8') as errfile:
                        error_writer = csv.DictWriter(errfile, fieldnames=error_fieldnames, extrasaction='ignore')
                        error_writer.writeheader()
                        error_writer.writerows(error_details)
                except IOError as e:
                     print(f"Error writing error file '{errors_path}': {e}")


        except FileNotFoundError as e:
            raise FileProcessingError(f"File not found: {e.filename}", file_path=e.filename) from e
        except IOError as e:
            # Catch I/O errors during file open/read/write
            raise FileProcessingError(f"File I/O error: {e}", file_path=getattr(e, 'filename', 'N/A')) from e
        except csv.Error as e:
            # Catch CSV parsing errors (e.g., malformed CSV)
            # Trying to determine row number here can be tricky
            raise FileProcessingError(f"CSV format error in '{input_path}': {e}", file_path=input_path) from e
        except CrispTransformerError as e:
            # Catch known configuration or processing errors from our library
            print(f"Processing failed due to configuration/operation error: {e}")
            raise # Re-raise to signal failure
        except Exception as e:
            # Catch any other unexpected errors during setup or processing
            print(f"An unexpected error occurred: {e}")
            raise FileProcessingError(f"Unexpected error during processing: {e}") from e
        finally:
            # --- Final Summary ---
            print("\n--- Processing Summary ---")
            print(f"Total rows read: {processed_count}")
            print(f"Rows successfully transformed: {success_count}")
            print(f"Rows with errors: {error_count}")
            print("--------------------------\n")


