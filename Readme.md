 # Crisp Data Transformer

**Crisp Data Transformer** is a Python library and command‑line tool for applying configurable, row‑by‑row transformations to CSV data. Using an external JSON file you can rename and parse columns, combine date fields, convert text to title case, add fixed values and more—all without touching a line of Python.

---

## Background

This tool was developed in response to the **Crisp Back End Take Home Test**, which specifies:
- Row-wise ETL style transformations of delimited text data.
- Configuration via an external JSON file (no hard‑coded logic).
- No significant third‑party ETL libraries; rely on standard language features.
- Capture and report invalid rows without stopping processing.
- Ability to handle very large datasets efficiently.

All assumptions and simplifications made during development are listed in the section below.
---

## Quick Start

1. Clone the repo  
   ```bash
   git clone https://github.com/hussin1147/Crisp.git
   cd Crisp
   ```

2. Edit `config.json`  
   Define:
   - `config_version`
   - `target_columns` (the schema & column order for your output CSV)
   - `transformations` (the sequence of operations)

3. Run the transformer  
   ```bash
   python3 main.py \
     --config config.json \
     --input orders.csv \
     --output output.csv \
     --errors errors.csv \
     --progress 1000
   ```

4. Inspect the results  
   - `output.csv` — your transformed data, with columns in the order you specified.  
   - `errors.csv` — any rows that failed, along with `row_number`, `error_message`, `failed_column` and the original fields.

---

## Installation

- Python 3.6+  
- No third‑party dependencies—everything runs on the standard library.

(Optional) Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

---

## Configuration

Below is a minimal example of `config.json`:

```json
{
  "config_version": "1.0",
  "target_columns": ["OrderID","OrderDate","ProductId","ProductName","Quantity","Unit"],
  "transformations": [
    {
      "operation": "rename_and_parse",
      "source_column": "Order Number",
      "target_column": "OrderID",
      "target_type": "integer"
    },
    {
      "operation": "combine_and_parse_date",
      "source_columns": ["Year","Month","Day"],
      "target_column": "OrderDate",
      "date_format": "%Y-%m-%d"
    },
    {
      "operation": "rename_proper_case_and_parse",
      "source_column": "Product Name",
      "target_column": "ProductName",
      "target_type": "string"
    },
    {
      "operation": "rename_and_parse",
      "source_column": "Count",
      "target_column": "Quantity",
      "target_type": "decimal",
      "parse_options": { "locale": "en_US" }
    },
    {
      "operation": "add_fixed_value",
      "target_column": "Unit",
      "value": "kg",
      "target_type": "string"
    }
  ]
}
```

### Supported operations

- **rename_and_parse**: Rename a field and convert its value to one of: `integer`, `decimal`, `string`, `date`.
- **combine_and_parse_date**: Stitch together multiple columns into a single date field using your `date_format`.
- **rename_proper_case_and_parse**: Rename, convert to Title Case, then parse (typically into a `string`).
- **add_fixed_value**: Add a brand‑new column with the same parsed value in every row.

For full details on parameters and behavior, see `crisp_transformer/operations.py`.

---

## Usage

```bash
python3 main.py \
  --config <config.json> \
  --input  <input.csv>  \
  --output <output.csv> \
  [--errors  <errors.csv>] \
  [--progress N]
```

- `--errors`: optional CSV path for rows that failed.
- `--progress`: print a message every N rows (default `1000`; set to `0` to disable).

---

## Examples

The repo ships with sample files:
- `orders.csv`         — sample input
- `output.csv`         — resulting output from a test run
- `actual_output.csv`  — the “golden” expected result

---

## Architecture & Technology Choices

- **crisp_transformer** (package):
  - `config_loader.py`  — loads & validates your JSON.
  - `operations.py`     — contains each transformation plus the factory.
  - `transformer.py`    — `DataTransformer` orchestrates the row‑by‑row work.
  - `exceptions.py`     — custom exceptions for clear error handling.
- **main.py**: CLI entry point built with `argparse`.

**Language & libraries**
- Python 3.6+ chosen for its batteries‑included stdlib (csv, json, datetime, decimal).
- No external dependencies to keep it lightweight and portable.

---

## Testing

A quick smoke test for your config:

```bash
python3 test_load.py
```

For full coverage, implement unit tests for individual handlers and integration tests for `DataTransformer.process()`.

---

## Error Handling

- **ConfigError**: invalid or missing config keys
- **FileProcessingError**: I/O or CSV format errors
- **OperationError/ValidationError**: per‑row failures (captured in your `--errors` file)

---

## Assumptions & Simplifications

- Only CSV input/output (UTF-8) is supported.
- Date combination assumes Year/Month/Day order, uses hyphens.
- Decimal locale support limited to `en_US` (commas removed).
- Proper-case uses `str.title()`, may not handle acronyms.
- Fixed-value operations assume the provided `value` can be parsed to the target type.
- Configuration is a JSON file, version locked to `1.0`.

---

## Next Steps

- Add support for custom delimiters & encodings.
- Enhance locale-aware number/date parsing.
- Introduce conditional transforms, lookups, regex extraction.
- Validate config.json against a JSON Schema.
- Replace `print` with a proper logging framework.
- Parallelize or batch process large files for performance.
- Package as a pip installable library with CI pipelines.
- Expand test suite with pytest or similar.

---

## Contributing

Contributions are welcome:

1. Fork the repository.
2. Create a descriptive feature branch.
3. Open a Pull Request (with tests if you can).

---