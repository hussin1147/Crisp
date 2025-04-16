# crisp_transformer/exceptions.py
# Defines custom exception classes for the data transformation library.

class CrispTransformerError(Exception):
  """Base class for all custom exceptions in this library."""
  pass

class ConfigError(CrispTransformerError):
  """Exception raised for errors in the configuration file.

  Attributes:
      message -- explanation of the error
      config_path -- path to the configuration file where the error occurred
  """
  def __init__(self, message, config_path=None):
    self.message = message
    self.config_path = config_path
    super().__init__(f"Configuration Error: {message}" + (f" in {config_path}" if config_path else ""))

class TransformError(CrispTransformerError):
  """Exception raised for errors during data transformation.

  Attributes:
      message -- explanation of the error
      row_number -- the row number (1-based) where the error occurred
      row_data -- the original data of the row causing the error
      column -- the specific column involved in the error (optional)
  """
  def __init__(self, message, row_number=None, row_data=None, column=None):
    self.message = message
    self.row_number = row_number
    self.row_data = row_data
    self.column = column
    details = []
    if row_number is not None:
      details.append(f"Row {row_number}")
    if column is not None:
      details.append(f"Column '{column}'")
    # Avoid printing potentially huge row_data in the default message
    super().__init__(f"Transformation Error: {message}" + (f" ({', '.join(details)})" if details else ""))

class OperationError(TransformError):
    """Specific transformation error related to a defined operation."""
    pass

class ValidationError(TransformError):
    """Specific transformation error related to data validation/parsing."""
    pass

class FileProcessingError(CrispTransformerError):
    """Exception raised for errors during file reading or writing."""
    def __init__(self, message, file_path=None):
        self.message = message
        self.file_path = file_path
        super().__init__(f"File Processing Error: {message}" + (f" for file {file_path}" if file_path else ""))

