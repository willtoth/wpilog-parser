# WPILog Parser Test Suite

This document describes the comprehensive test suite for the WPILog parser, based on the official WPILib Data Log File Format Specification Version 1.0.

## Test Coverage Summary

**Total Tests: 68**
- Unit tests (datalog_tests.rs): 49 tests
- Integration tests (formatter_tests.rs): 18 tests
- Library unit tests: 1 test

All tests pass successfully.

## Test Organization

### Test Utilities (`tests/common/mod.rs`)

The `WpilogBuilder` helper provides a fluent API for creating valid WPILOG test files:

```rust
let data = WpilogBuilder::new()
    .start_record(1_000_000, 1, "test", "int64", "")
    .int64_record(1, 1_100_000, 42)
    .build();
```

Features:
- Variable-length encoding optimization for entry IDs, timestamps, and payload sizes
- Support for all WPILog data types
- Control record builders (Start, Finish, Set Metadata)
- UTF-8 string support
- Custom header versions and extra headers

## Test Categories

### 1. Header Parsing Tests (`datalog_tests.rs`)

Tests the parsing of the WPILOG file header according to spec:

- **Valid headers**: Minimal header, headers with extra header strings
- **Invalid headers**: Corrupted magic bytes, old versions, truncated files
- **Edge cases**: Empty files, UTF-8 extra headers, different version numbers

**Coverage**:
- ✅ Magic bytes validation ("WPILOG")
- ✅ Version number parsing (major/minor)
- ✅ Extra header length and content parsing
- ✅ Error handling for malformed headers

### 2. Control Record Tests (`datalog_tests.rs`)

Tests the three control record types defined in the spec:

#### Start Records (Type 0)
- Basic start records with entry ID, name, and type
- Start records with metadata (JSON)
- UTF-8 entry names and metadata
- Complete entry lifecycle (start → data → finish)

#### Finish Records (Type 1)
- Basic finish records
- Entry ID reuse after finish

#### Set Metadata Records (Type 2)
- Metadata updates during entry lifetime

**Coverage**:
- ✅ Control record type detection
- ✅ Entry ID management
- ✅ Metadata parsing and updates
- ✅ UTF-8 string support in names, types, and metadata

### 3. Data Type Tests (`datalog_tests.rs`)

Comprehensive tests for all standard WPILog data types:

#### Scalar Types
- `boolean`: true/false values
- `int64`: Positive, negative, min/max values
- `float`: 32-bit IEEE-754 floating point
- `double`: 64-bit IEEE-754 floating point
- `string`: Empty, ASCII, UTF-8 strings

#### Array Types
- `boolean[]`: Boolean arrays including empty arrays
- `int64[]`: Integer arrays with positive/negative values
- `float[]`: Float arrays
- `double[]`: Double arrays with high precision
- `string[]`: String arrays with UTF-8 support

**Coverage**:
- ✅ All standard data types from spec
- ✅ Edge cases: empty arrays, max/min values, UTF-8
- ✅ Proper byte-order handling (little endian)
- ✅ Array length inference from payload size

### 4. Variable-Length Encoding Tests (`datalog_tests.rs`)

Tests the variable-length encoding scheme for record headers:

- **Entry IDs**: 1-byte (≤255), 2-byte (≤65535), 3-byte (≤16777215), 4-byte
- **Timestamps**: 1-byte to 8-byte encodings
- **Payload sizes**: Automatic size optimization

**Coverage**:
- ✅ Header bitfield parsing (entry_len, size_len, timestamp_len)
- ✅ Varint decoding for different byte lengths
- ✅ Optimal encoding selection in builder

### 5. Edge Case Tests (`datalog_tests.rs`)

Tests non-standard but valid scenarios:

- **Out-of-order timestamps**: Records with non-monotonic timestamps
- **Entry ID reuse**: Reusing entry IDs after Finish records
- **Multiple concurrent entries**: Interleaved records from different entries
- **Large payloads**: 10KB+ string payloads
- **Many records**: 1000+ records in single file
- **Zero timestamps**: Valid edge case

**Coverage**:
- ✅ No timestamp ordering requirement
- ✅ Entry ID lifecycle management
- ✅ Multi-entry interleaving
- ✅ Scalability (1000+ records)

### 6. Integration Tests (`formatter_tests.rs`)

End-to-end tests of the full parsing pipeline:

#### Full Pipeline Tests
- Single and multiple entry parsing
- All scalar types in one file
- Array type parsing
- Entry lifecycle with Finish records
- Large datasets (1000+ records)

#### Formatter-Specific Tests
- Loop count increment tracking (`/Timestamp` entry)
- UTF-8 name and value handling
- JSON type support
- Metadata tracking
- Empty file handling

#### Struct Schema Tests
- Schema string parsing (`double x; double y`)
- Enum handling in schemas
- Column name sanitization

**Coverage**:
- ✅ Two-pass processing (schema inference → data reading)
- ✅ Wide row format generation
- ✅ Loop count management
- ✅ Type-specific data extraction
- ✅ Error handling for invalid files

## Running the Tests

```bash
# Run all tests
cargo test

# Run specific test suite
cargo test --test datalog_tests
cargo test --test formatter_tests

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_parse_all_scalar_types
```

## Test Data Validation

All test data is generated programmatically using `WpilogBuilder` to ensure:
1. Correct byte ordering (little endian)
2. Proper variable-length encoding
3. Valid UTF-8 strings
4. Spec-compliant record structure

## Spec Compliance

This test suite validates compliance with:
- WPILib Data Log File Format Specification Version 1.0
- Kaitai Struct definition (wpilog.ksy)

### Verified Spec Requirements

✅ **Header Format**
- 6-byte magic string "WPILOG"
- 16-bit version number (0x0100 for v1.0)
- 32-bit extra header length
- UTF-8 extra header string

✅ **Record Format**
- 1-byte header length bitfield
- Variable-length entry ID (1-4 bytes)
- Variable-length payload size (1-4 bytes)
- Variable-length timestamp (1-8 bytes)
- Payload data

✅ **Control Records**
- Entry ID 0 reserved for control
- Start (type 0), Finish (type 1), Set Metadata (type 2)
- Proper string encoding with 32-bit length prefix

✅ **Data Types**
- All standard types: boolean, int64, float, double, string
- All array types: boolean[], int64[], float[], double[], string[]
- Little endian byte order
- Array length inference from payload size

✅ **Design Requirements**
- No timestamp ordering requirement
- Entry ID reuse after Finish
- No padding between records
- UTF-8 encoding for all strings

## Known Limitations

The test suite currently does not cover:
- `raw` data type (not used in formatter)
- `msgpack` data type (basic coverage only)
- Protocol buffer types (`proto:*`)
- Struct types with nested data (basic coverage only)
- Struct arrays

These are left as TODOs in the implementation and would require additional test cases.

## Adding New Tests

When adding tests:

1. Use `WpilogBuilder` for creating test data
2. Follow the naming convention: `test_<feature>_<scenario>`
3. Group related tests with comments
4. Test both success and failure cases
5. Verify against the spec section being tested

Example:

```rust
#[test]
fn test_double_array_empty() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/test", "double[]", "")
        .double_array_record(1, 1_100_000, &[])
        .build();

    let reader = DataLogReader::new(data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_double_array().unwrap();
    assert_eq!(values, Vec::<f64>::new());
}
```

## CI/CD Integration

To integrate with continuous integration:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: cargo test --all

- name: Run tests with coverage
  run: cargo test --all -- --nocapture
```

## Performance

Test suite execution time: ~1 second total
- Unit tests: <100ms
- Integration tests: <100ms
- Library compilation: ~20-30s (first run)

The test suite is designed for fast feedback during development.
