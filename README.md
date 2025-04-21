# PolarsDB

A lightweight CSV-based database system built on Polars - optimized for analytics and data manipulation.

## Features

- **Simple CSV-based Storage**: Uses CSV files for storing table data, making it transparent and accessible
- **UPSERT Support**: Full support for updating existing records and inserting new ones
- **Powerful Query Interface**: Query data using the full power of Polars expressions
- **Operation History**: Comprehensive history tracking of all database operations
- **Flexible Configuration**: Simple configuration system for managing tables and settings

## Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/yourusername/PolarsDB.git
cd PolarsDB
pip install -r requirements.txt
```

## Quick Start

```python
from src.config import DBConfig
from src.client import DBClient
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

# Insert data
df = pl.DataFrame(customer_data)
client.insert_data("customers", df)

# Query data
high_value_customers = client.query("customers.filter(pl.col('total_spent') > 100)")
print(high_value_customers)
```

## Core Operations

### Insert/Upsert Data

```python
# Insert new data (will update existing records based on deduplication columns)
df = pl.DataFrame(data)
client.insert_data("table_name", df)
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
# Delete by date range
client.delete_data_by_date("table_name", "date_column", start_date, end_date)

# Delete by key
client.delete_data_by_key("table_name", "key_column", "key_value")
```

### Operation History

```python
# Get history for a specific table
history = client.get_operation_history("table_name")

# Get all operation history
all_history = client.get_all_operation_history()
```

## Testing

The repository includes a comprehensive test suite:

```bash
python test.py
```

This will run through basic operations and demonstrates the functionality of the library.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 