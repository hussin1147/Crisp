# crisp_transformer/config_loader.py
# Handles loading and basic validation of the JSON configuration file.

import json
from .exceptions import ConfigError, FileProcessingError

SUPPORTED_CONFIG_VERSION = "1.0"

def load_config(config_path: str) -> dict:
  """
  Loads, parses, and validates the transformation configuration from a JSON file.

  Args:
      config_path: The path to the JSON configuration file.

  Returns:
      A dictionary representing the validated configuration.

  Raises:
      FileNotFoundError: If the config file does not exist.
      FileProcessingError: If there's an error reading the file.
      ConfigError: If the JSON is invalid or the configuration fails validation checks.
  """
  try:
    with open(config_path, 'r', encoding='utf-8') as f:
      config_data = json.load(f)
  except FileNotFoundError:
    # Re-raise FileNotFoundError as it's a standard Python exception
    # users might expect to catch specifically.
    raise FileNotFoundError(f"Configuration file not found at {config_path}")
  except json.JSONDecodeError as e:
    raise ConfigError(f"Invalid JSON format: {e}", config_path=config_path) from e
  except IOError as e:
    raise FileProcessingError(f"Cannot read configuration file: {e}", file_path=config_path) from e

  # --- Basic Validation ---
  if not isinstance(config_data, dict):
      raise ConfigError("Configuration root must be a JSON object.", config_path=config_path)

  # Check version
  version = config_data.get("config_version")
  if version != SUPPORTED_CONFIG_VERSION:
      raise ConfigError(f"Unsupported configuration version '{version}'. Expected '{SUPPORTED_CONFIG_VERSION}'.", config_path=config_path)

  # Check top-level keys
  required_keys = {"target_columns", "transformations"}
  missing_keys = required_keys - set(config_data.keys())
  if missing_keys:
      raise ConfigError(f"Missing required configuration keys: {', '.join(missing_keys)}", config_path=config_path)

  # Check types of main sections
  if not isinstance(config_data.get("target_columns"), list):
      raise ConfigError("'target_columns' must be a list.", config_path=config_path)
  if not isinstance(config_data.get("transformations"), list):
      raise ConfigError("'transformations' must be a list.", config_path=config_path)

  # Basic validation of transformation steps (more detailed validation can happen later)
  for i, step in enumerate(config_data.get("transformations", [])):
      if not isinstance(step, dict):
          raise ConfigError(f"Transformation step {i+1} must be an object.", config_path=config_path)
      if "operation" not in step:
          raise ConfigError(f"Transformation step {i+1} is missing the 'operation' key.", config_path=config_path)
      # Further validation per operation type will occur in the transformer/operations modules

  print(f"Configuration loaded and validated successfully from {config_path}")
  return config_data

