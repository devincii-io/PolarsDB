"""
Shared pytest fixtures for PolarsDB tests
"""
import pytest
import tempfile
import os
from pathlib import Path
import polars as pl

# Add the local PolarsDB to path for testing
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PolarsDB import DBConfig, DBClient


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def config_path(temp_dir):
    """Create a temporary config file path"""
    return os.path.join(temp_dir, "config.json")


@pytest.fixture
def data_path(temp_dir):
    """Create a temporary data directory path"""
    return os.path.join(temp_dir, "data")


@pytest.fixture
def db_config(config_path, data_path):
    """Create a DBConfig instance for testing"""
    return DBConfig(config_path, data_path)


@pytest.fixture
def db_client(db_config):
    """Create a DBClient instance for testing"""
    return DBClient(db_config)


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing"""
    return pl.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'diana@test.com', 'eve@test.com']
    })


@pytest.fixture
def users_table_config():
    """Standard users table configuration"""
    return {
        'deduplication_columns': ['id', 'email']
    }


@pytest.fixture
def orders_table_config():
    """Standard orders table configuration"""
    return {
        'deduplication_columns': ['order_id']
    }


@pytest.fixture
def setup_test_tables(db_config, users_table_config, orders_table_config):
    """Setup standard test tables"""
    db_config.add_table('users', users_table_config)
    db_config.add_table('orders', orders_table_config)
    return db_config 