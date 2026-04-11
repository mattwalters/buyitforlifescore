#!/bin/bash
# ZST to Parquet Converter (DuckDB)
#
# Usage:
#   This script converts a local Zstandard compressed JSON file (.zst) 
#   into a highly optimized Parquet format using DuckDB.
#
# Prerequisites:
#   - DuckDB installed (`brew install duckdb` on Mac)
#
# Example Run:
#   ./scripts/convert_zst_to_parquet.sh ./data/BuyItForLife_comments.zst
#   (This will output ./data/BuyItForLife_comments.parquet)
#

set -e

# 1. Configuration & Input Validation
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <input_file.zst>"
    exit 1
fi

INPUT_FILE="$1"

if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: File '$INPUT_FILE' not found."
    exit 1
fi

# Determine the output filename
# Strip the .zst or .json.zst extension and append .parquet
if [[ "$INPUT_FILE" == *.json.zst ]]; then
    OUTPUT_FILE="${INPUT_FILE%.json.zst}.parquet"
elif [[ "$INPUT_FILE" == *.zst ]]; then
    OUTPUT_FILE="${INPUT_FILE%.zst}.parquet"
else
    OUTPUT_FILE="${INPUT_FILE}.parquet"
fi

# 2. Check for DuckDB
if ! command -v duckdb &> /dev/null; then
    echo "Error: DuckDB is not installed."
    echo "Please run: 'brew install duckdb' or visit https://duckdb.org/docs/installation/"
    exit 1
fi

echo "Starting conversion..."
echo "  Source : $INPUT_FILE"
echo "  Target : $OUTPUT_FILE"
echo "  Engine : DuckDB"
echo "----------------------------------------------------"

# 3. The DuckDB Execution
duckdb -c "
-- Install and load necessary extensions
INSTALL json; 
LOAD json;

-- Execute the conversion
-- read_ndjson_auto handles the local .zst decompression and schema inference.
-- By setting sample_size=-1, we tell DuckDB to scan the entire file first
-- to build a complete schema, preventing 'unknown key' errors on messy data.
COPY (
    SELECT * FROM read_ndjson_auto('$INPUT_FILE', sample_size=-1)
) TO '$OUTPUT_FILE' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
"

echo "----------------------------------------------------"
echo "Success! Your optimized Parquet data is ready in: $OUTPUT_FILE"
echo "You can now query it instantly:"
echo "duckdb -c \"SELECT * FROM '$OUTPUT_FILE' LIMIT 5;\""
