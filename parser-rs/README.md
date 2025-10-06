# WPILog Parser (Rust)

A high-performance Rust implementation of the WPILog parser, converting `.wpilog` files to Parquet format.

## Features

- Fast, memory-efficient parsing of WPILib data log files
- Support for all WPILog data types (boolean, int64, double, float, string, arrays, msgpack, structs)
- Parquet output format with chunking for large files
- Wide and long output formats
- Struct schema parsing and unpacking
- Memory-mapped file access for efficient reading

## Building

```bash
cargo build --release
```

## Usage

Parse all `.wpilog` files in a directory:

```bash
cargo run --release -- <INPUT_DIR> --out-root <OUTPUT_DIR>
```

### Options

- `<INPUT_DIR>`: Directory containing `.wpilog` files (required)
- `--out-root <OUTPUT_DIR>`: Root output directory for converted files (required)
- `--file-format <FORMAT>`: Output file format (default: `parquet`)
  - `parquet`: Apache Parquet format
  - `avro`: Apache Avro format (not yet implemented)
  - `json`: JSON format (not yet implemented)
- `--output-format <FORMAT>`: Output data format (default: `wide`)
  - `wide`: Wide format with each metric as a column
  - `long`: Long format with nested values (not fully implemented)

### Example

```bash
cargo run --release -- ./input-logs --out-root ./output --file-format parquet --output-format wide
```

## Architecture

- `src/datalog.rs`: Core binary parser for WPILog format
- `src/models.rs`: Data structures and schema definitions
- `src/formatter.rs`: Record parsing and transformation logic
- `src/formats/parquet.rs`: Parquet output writer
- `src/main.rs`: CLI entry point

## Output

The parser creates a directory for each input `.wpilog` file, containing chunked Parquet files (50,000 rows per chunk by default).

Output directory structure:
```
output/
├── log1/
│   ├── file_part000.parquet
│   ├── file_part001.parquet
│   └── ...
└── log2/
    ├── file_part000.parquet
    └── ...
```

## Performance

The Rust implementation provides significant performance improvements over the Python version:
- Zero-copy parsing with memory-mapped files
- No GIL limitations
- Efficient memory usage
- Fast Parquet writing with Apache Arrow

## License

This project is based on WPILib's data log format. See the original Python implementation in the `parser` directory.
