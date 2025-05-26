# PolarsDB Tests

This directory contains the test suite for PolarsDB, organized using pytest best practices.

## Test Structure

```
tests/
├── __init__.py              # Makes tests a package
├── conftest.py              # Shared pytest fixtures
├── test_config.py           # Tests for DBConfig class
├── test_client.py           # Tests for DBClient class
├── integration/             # Integration tests
│   ├── __init__.py
│   └── test_full_workflow.py
└── README.md               # This file
```

## Test Categories

### Unit Tests
- **`test_config.py`**: Tests for the `DBConfig` class including configuration management, table operations, and file I/O
- **`test_client.py`**: Tests for the `DBClient` class including data insertion, reading, and operation history

### Integration Tests
- **`test_full_workflow.py`**: End-to-end tests that verify complete workflows including multiple table operations, duplicate detection, and error handling

## Running Tests

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test Categories
```bash
# Unit tests only
pytest tests/test_*.py -v

# Integration tests only
pytest tests/integration/ -v
```

### Run Tests with Markers
```bash
# Run only integration tests
pytest -m integration -v

# Skip slow tests
pytest -m "not slow" -v
```

### Run Specific Test Files
```bash
pytest tests/test_config.py -v
pytest tests/test_client.py -v
```

### Run Specific Test Methods
```bash
pytest tests/test_config.py::TestDBConfig::test_config_creation_with_valid_paths -v
```

## Test Configuration

The test configuration is defined in `pytest.ini`:
- Test discovery patterns
- Output formatting
- Custom markers for categorizing tests
- Warning suppression

## Fixtures

Shared fixtures are defined in `conftest.py`:
- **`temp_dir`**: Temporary directory for test isolation
- **`config_path`**: Temporary config file path
- **`data_path`**: Temporary data directory path
- **`db_config`**: Pre-configured DBConfig instance
- **`db_client`**: Pre-configured DBClient instance
- **`users_table_config`**: Sample table configuration
- **`sample_dataframe`**: Sample test data

## Test Data

Tests use temporary directories and files to ensure:
- Test isolation (no interference between tests)
- Clean environment for each test
- No pollution of the actual file system

## Coverage

The test suite covers:
- ✅ Configuration management and validation
- ✅ Table creation and management
- ✅ Data insertion with duplicate detection and immediate feedback
- ✅ Data reading and querying
- ✅ Operation history tracking
- ✅ Error handling and edge cases
- ✅ File I/O operations
- ✅ Path handling and normalization
- ✅ Complete end-to-end workflows
- ✅ Operation record return values for insert/delete operations

## Adding New Tests

When adding new tests:
1. Follow the existing naming conventions (`test_*.py`)
2. Use appropriate fixtures from `conftest.py`
3. Add integration tests for complex workflows
4. Use descriptive test names and docstrings
5. Test both success and error cases
6. Use temporary directories for file operations 