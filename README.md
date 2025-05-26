# PolarsDB

A lightweight CSV-based database system built on Polars - optimized for analytics and data manipulation.

## Features

- **Simple CSV-based Storage**: Uses CSV files for storing table data, making it transparent and accessible
- **UPSERT Support**: Full support for updating existing records and inserting new ones with immediate feedback
- **Powerful Query Interface**: Query data using the full power of Polars expressions
- **Operation History**: Comprehensive history tracking of all database operations
- **Immediate Feedback**: Insert and delete operations return detailed operation records with statistics
- **Flexible Configuration**: Simple configuration system for managing tables and settings

## Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/devincii-io/PolarsDB.git
cd PolarsDB
pip install -e .
```

Or install directly from GitHub:

```bash
pip install git+https://github.com/devincii-io/PolarsDB.git
```

## Quick Start

```python
from PolarsDB import DBConfig, DBClient
import polars as pl

# Create a configuration
config = DBConfig(
    config_path="config.json",
    data_path="data",
    tables={
        "customers": {
            "deduplication_columns": ["customer_id"]
        }
    }
)

# Create a client
client = DBClient(config)

# Create sample data
customer_data = {
    "customer_id": ["CUST001", "CUST002", "CUST003"],
    "name": ["Alice", "Bob", "Charlie"],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "total_spent": [150.50, 320.75, 95.20]
}

# Insert data (returns operation record with immediate feedback)
df = pl.DataFrame(customer_data)
result = client.insert_data("customers", df)

# Check operation results
print(f"✅ {result['summary']}")
print(f"📊 Inserted {result['statistics']['rows_newly_inserted']} rows")

# Insert some updates
update_data = pl.DataFrame({
    "customer_id": ["CUST001", "CUST004"],  # CUST001 exists, CUST004 is new
    "name": ["Alice Smith", "Diana"],
    "email": ["alice.smith@example.com", "diana@example.com"],
    "total_spent": [200.00, 75.30]
})

result = client.insert_data("customers", update_data)
print(f"✅ {result['summary']}")
print(f"📊 New: {result['statistics']['rows_newly_inserted']}, Updated: {result['statistics']['rows_updated']}")

# Query data
high_value_customers = client.query("customers.filter(pl.col('total_spent') > 100)")
print(high_value_customers)
```

## Core Operations

### Insert/Upsert Data

```python
# Insert new data (will update existing records based on deduplication columns)
df = pl.DataFrame(data)
result = client.insert_data("table_name", df)

# Get immediate feedback about the operation
print(f"Operation completed: {result['summary']}")
print(f"Statistics: {result['statistics']}")

# Access specific metrics
rows_added = result['statistics']['rows_newly_inserted']
duplicates = result['statistics']['duplicates_found']
table_size = result['statistics']['new_table_size']
```

### Query Data

```python
# Simple filter
result = client.query("customers.filter(pl.col('age') > 30)")

# Join tables
result = client.query("customers.join(orders, on='customer_id')")

# Aggregation
result = client.query("customers.group_by('region').agg(pl.col('total_spent').sum())")

# Parameterized query
result = client.query(
    "customers.filter(pl.col('total_spent') > threshold)",
    {"threshold": 1000}
)
```

### Delete Data

```python
# Delete by date range (returns operation record)
result = client.delete_data_by_date("table_name", "date_column", start_date, end_date)
print(f"Deleted {result['statistics']['rows_deleted']} rows")

# Delete by key (returns operation record)
result = client.delete_data_by_key("table_name", "key_column", "key_value")
print(f"Operation: {result['summary']}")
print(f"Rows deleted: {result['statistics']['rows_deleted']}")
print(f"New table size: {result['statistics']['new_table_size']}")
```

### Operation Records & History

All data modification operations (`insert_data`, `delete_data_by_date`, `delete_data_by_key`) return detailed operation records:

```python
# Insert data and get immediate feedback
result = client.insert_data("customers", df)

# Operation record contains:
print(result['operation'])      # "insert_data"
print(result['timestamp'])      # When it happened
print(result['summary'])        # Human-readable summary
print(result['statistics'])     # Detailed metrics
print(result['args'])          # Operation arguments

# Available statistics include:
stats = result['statistics']
print(f"Input rows: {stats['input_rows']}")
print(f"New rows inserted: {stats['rows_newly_inserted']}")
print(f"Duplicates found: {stats['duplicates_found']}")
print(f"Rows updated: {stats['rows_updated']}")
print(f"Final table size: {stats['new_table_size']}")
print(f"Duplication rate: {stats['duplication_rate']}%")
```

You can also access historical operations:

```python
# Get history for a specific table
history = client.get_operation_history("table_name")

# Get all operation history
all_history = client.get_all_operation_history()

# Get recent operations
recent = client.get_recent_operations("table_name", limit=5)
```

## Testing

The repository includes a comprehensive test suite using pytest:

```bash
# Run all tests
pytest tests/ -v

# Run only unit tests
pytest tests/test_*.py -v

# Run only integration tests
pytest tests/integration/ -v

# Run tests with markers
pytest -m integration -v
```

The test suite covers:
- Configuration management and validation
- Data insertion with duplicate detection and immediate feedback
- Data reading and querying
- Operation history tracking
- Error handling and edge cases
- Complete end-to-end workflows
- Operation record return values

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 