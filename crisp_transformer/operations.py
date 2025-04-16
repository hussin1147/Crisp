# crisp_transformer/operations.py
# Contains individual transformation functions and a factory to dispatch to them.

import datetime
import decimal
# import locale # Consider using locale for more robust number parsing if needed
from typing import Callable, Dict, Any, List
from .exceptions import ConfigError, OperationError, ValidationError

# --- Type Parsing Helpers ---

def _parse_integer(value: str, operation_config: Dict[str, Any]) -> int:
  """Attempts to parse a string value into an integer."""
  if value is None or value.strip() == '':
      raise ValidationError("Value is empty, cannot parse as integer")
  try:
      return int(value.strip())
  except ValueError:
      raise ValidationError(f"Cannot parse '{value}' as integer")

def _parse_decimal(value: str, operation_config: Dict[str, Any]) -> decimal.Decimal:
  """
  Attempts to parse a string value into a decimal.Decimal.
  Handles basic locale-aware parsing if specified in options.
  """
  if value is None or value.strip() == '':
      raise ValidationError("Value is empty, cannot parse as decimal")

  parse_options = operation_config.get("parse_options", {})
  input_value = value.strip()

  # Basic handling for common 'en_US' style commas.
  # A more robust solution might use the locale module, but that has global state implications.
  if parse_options.get("locale") == "en_US":
      input_value = input_value.replace(',', '')
      # Potentially add more locale-specific preprocessing here if needed

  try:
      return decimal.Decimal(input_value)
  except decimal.InvalidOperation:
      raise ValidationError(f"Cannot parse '{value}' as decimal")

def _parse_date(value: str, operation_config: Dict[str, Any]) -> datetime.date:
  """
  Attempts to parse a string value into a date using the specified format.
  Requires 'date_format' in operation_config.
  """
  if value is None or value.strip() == '':
      raise ValidationError("Value is empty, cannot parse as date")

  date_format = operation_config.get("date_format")
  if not date_format:
      raise ConfigError("Missing 'date_format' in configuration for date parsing operation")

  try:
      # Assuming date only, use strptime and then .date()
      return datetime.datetime.strptime(value.strip(), date_format).date()
  except ValueError:
      raise ValidationError(f"Cannot parse '{value}' as date with format '{date_format}'")

def _parse_string(value: str, operation_config: Dict[str, Any]) -> str:
  """Parses (or rather, validates/cleans) a string value."""
  if value is None:
      # Depending on requirements, None might be acceptable for strings,
      # or we might want to default to empty string. Let's default to empty.
      return ""
  # Basic stripping of whitespace
  return str(value).strip()

# Mapping of target types to parsing functions
TYPE_PARSERS = {
  "integer": _parse_integer,
  "decimal": _parse_decimal,
  "date": _parse_date,
  "string": _parse_string,
  # Add other types like 'datetime', 'boolean' here if needed
}

# --- String Manipulation Helpers ---

def _proper_case(value: str) -> str:
  """Converts a string to proper case (Title Case)."""
  if value is None:
      return ""
  # Simple title casing. More complex rules might be needed for specific edge cases.
  return str(value).strip().title()


# --- Operation Handler Functions ---
# Each handler receives:
# - row: The current data row (dict) being processed.
# - config: The specific configuration dict for this operation step.
# - row_number: The current row number (for error reporting).
# It should modify the 'row' dictionary in place or return a new value
# if it's an operation that adds a new column without a direct source.

def handle_rename_and_parse(row: Dict[str, Any], config: Dict[str, Any], row_number: int) -> None:
  """Handles renaming a column and parsing its value to the target type."""
  source_col = config.get("source_column")
  target_col = config.get("target_column")
  target_type = config.get("target_type")

  if not all([source_col, target_col, target_type]):
      raise ConfigError("Rename/Parse operation missing source_column, target_column, or target_type")

  if source_col not in row:
      raise OperationError(f"Source column '{source_col}' not found in input row", column=source_col)

  parser = TYPE_PARSERS.get(target_type)
  if not parser:
      raise ConfigError(f"Unsupported target_type '{target_type}' specified")

  original_value = row.get(source_col)
  try:
      parsed_value = parser(original_value, config)
      # Add/overwrite target column, remove source if different
      row[target_col] = parsed_value
      if source_col != target_col:
          del row[source_col] # Remove original column if renamed
  except ValidationError as e:
      # Enrich error with context and re-raise as OperationError
      raise OperationError(e.message, column=source_col, row_number=row_number) from e


def handle_combine_and_parse_date(row: Dict[str, Any], config: Dict[str, Any], row_number: int) -> None:
  """Combines multiple source columns and parses the result as a date."""
  source_cols = config.get("source_columns")
  target_col = config.get("target_column")
  date_format = config.get("date_format") # Format for the *combined* string

  if not all([source_cols, target_col, date_format]):
      raise ConfigError("Combine/Date operation missing source_columns, target_column, or date_format")
  if not isinstance(source_cols, list) or len(source_cols) == 0:
      raise ConfigError("'source_columns' must be a non-empty list")

  combined_value_str = ""
  try:
      # Simple concatenation assuming Year, Month, Day order from example
      # A more robust version might allow specifying the concatenation pattern
      values_to_combine = []
      for col in source_cols:
          if col not in row:
              raise OperationError(f"Source column '{col}' not found for combining", column=col)
          value = row.get(col)
          if value is None or str(value).strip() == '':
              raise OperationError(f"Source column '{col}' is empty, cannot combine for date", column=col)
          values_to_combine.append(str(value).strip())

      # Example: Combine YYYY, MM, DD with hyphens for '%Y-%m-%d' format
      combined_value_str = "-".join(values_to_combine) # Adjust separator if needed

      # Use the date parser helper
      parsed_value = _parse_date(combined_value_str, config)
      row[target_col] = parsed_value

      # Remove original columns after successful combination
      for col in source_cols:
          if col in row: # Check existence before deleting
             del row[col]

  except ValidationError as e:
       # Enrich error with context and re-raise as OperationError
       raise OperationError(f"Error parsing combined value '{combined_value_str}': {e.message}", column=target_col, row_number=row_number) from e
  except OperationError as e:
       # Re-raise OperationErrors directly but ensure row_number is set
       e.row_number = row_number
       raise e


def handle_rename_proper_case_and_parse(row: Dict[str, Any], config: Dict[str, Any], row_number: int) -> None:
  """Handles renaming, proper casing, and parsing a string column."""
  source_col = config.get("source_column")
  target_col = config.get("target_column")
  target_type = config.get("target_type") # Should typically be 'string'

  if not all([source_col, target_col, target_type]):
      raise ConfigError("Rename/ProperCase/Parse operation missing source_column, target_column, or target_type")

  if target_type != 'string':
      # While possible, usually proper case implies string output. Warn or error? Let's allow but maybe log.
      print(f"Warning: Proper casing usually implies target_type 'string', but got '{target_type}' for column '{target_col}'.")
      # Or raise ConfigError("Proper casing operation should have target_type 'string'")

  if source_col not in row:
      raise OperationError(f"Source column '{source_col}' not found in input row", column=source_col)

  original_value = row.get(source_col)
  try:
      # Apply proper casing first
      cased_value = _proper_case(original_value)

      # Then parse (usually just validates/cleans for string)
      parser = TYPE_PARSERS.get(target_type)
      if not parser:
          raise ConfigError(f"Unsupported target_type '{target_type}' specified")

      parsed_value = parser(cased_value, config)

      # Add/overwrite target column, remove source if different
      row[target_col] = parsed_value
      if source_col != target_col:
          del row[source_col]
  except ValidationError as e:
      # Enrich error with context and re-raise as OperationError
      raise OperationError(e.message, column=source_col, row_number=row_number) from e


def handle_add_fixed_value(row: Dict[str, Any], config: Dict[str, Any], row_number: int) -> None:
  """Adds a new column with a fixed value, parsing it to the target type."""
  target_col = config.get("target_column")
  fixed_value = config.get("value")
  target_type = config.get("target_type")

  # Note: 'value' could be None or other types, handle appropriately
  if target_col is None or target_type is None: # value can legitimately be None
      raise ConfigError("Add Fixed Value operation missing target_column or target_type")

  parser = TYPE_PARSERS.get(target_type)
  if not parser:
      raise ConfigError(f"Unsupported target_type '{target_type}' specified")

  try:
      # Parse the fixed value itself to ensure type consistency
      # We pass the value as a string to the parser, unless it's None
      value_to_parse = str(fixed_value) if fixed_value is not None else None
      parsed_value = parser(value_to_parse, config)
      row[target_col] = parsed_value
  except ValidationError as e:
      # Error likely means the *fixed value* in config is incompatible with target_type
      raise ConfigError(f"Fixed value '{fixed_value}' is not compatible with target_type '{target_type}': {e.message}") from e


# --- Operation Factory ---

OPERATION_HANDLERS = {
  "rename_and_parse": handle_rename_and_parse,
  "combine_and_parse_date": handle_combine_and_parse_date,
  "rename_proper_case_and_parse": handle_rename_proper_case_and_parse,
  "add_fixed_value": handle_add_fixed_value,
  # Add new operation names and their handler functions here
}

def get_operation_handler(operation_config: Dict[str, Any]) -> Callable:
  """
  Factory function to get the appropriate operation handler based on config.
  Performs validation specific to the operation type.

  Args:
      operation_config: The configuration dictionary for a single transformation step.

  Returns:
      The callable handler function for that operation.

  Raises:
      ConfigError: If the operation type is unknown or required parameters for
                   that operation are missing in the config.
  """
  operation_name = operation_config.get("operation")
  if not operation_name:
      raise ConfigError("Operation configuration is missing the 'operation' key")

  handler = OPERATION_HANDLERS.get(operation_name)
  if not handler:
      raise ConfigError(f"Unknown operation type specified: '{operation_name}'")

  # --- Add operation-specific validation here if needed ---
  # Example: Ensure 'rename_and_parse' has source_column, target_column, target_type
  required_keys = {
      "rename_and_parse": ["source_column", "target_column", "target_type"],
      "combine_and_parse_date": ["source_columns", "target_column", "date_format"],
      "rename_proper_case_and_parse": ["source_column", "target_column", "target_type"],
      "add_fixed_value": ["target_column", "value", "target_type"],
  }
  keys_for_op = required_keys.get(operation_name)
  if keys_for_op:
      missing_keys = [k for k in keys_for_op if k not in operation_config]
      if missing_keys:
          raise ConfigError(f"Operation '{operation_name}' is missing required keys: {', '.join(missing_keys)}")

  # --- Date format validation ---
  if operation_name == "combine_and_parse_date" or operation_config.get("target_type") == "date":
      if "date_format" not in operation_config:
           raise ConfigError(f"Operation '{operation_name}' requires 'date_format' when dealing with dates.")
      # Could add validation for the format string itself if desired

  # --- Type validation ---
  if "target_type" in operation_config and operation_config["target_type"] not in TYPE_PARSERS:
       raise ConfigError(f"Unsupported target_type '{operation_config['target_type']}' specified for operation '{operation_name}'")


  return handler

