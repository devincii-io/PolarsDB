"""
Integration tests for PolarsDB full workflow
"""
import pytest
import polars as pl
import os
import tempfile
from pathlib import Path

# Add the local PolarsDB to path for testing
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from PolarsDB import DBConfig, DBClient


@pytest.mark.integration
class TestFullWorkflow:
    """Integration tests for complete PolarsDB workflows"""
    
    def test_complete_database_lifecycle(self):
        """Test a complete database lifecycle from creation to querying"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup
            config_path = os.path.join(temp_dir, "config.json")
            data_path = os.path.join(temp_dir, "data")
            
            # Create config and client
            config = DBConfig(config_path, data_path)
            client = DBClient(config)
            
            # Add multiple tables
            client.add_table('users', {'deduplication_columns': ['user_id', 'email']})
            client.add_table('orders', {'deduplication_columns': ['order_id']})
            client.add_table('products', {'deduplication_columns': ['product_id']})
            
            # Insert users data
            users_df = pl.DataFrame({
                'user_id': [1, 2, 3, 4, 5],
                'email': ['user1@test.com', 'user2@test.com', 'user3@test.com', 'user4@test.com', 'user5@test.com'],
                'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
                'age': [25, 30, 35, 28, 32],
                'city': ['New York', 'London', 'Tokyo', 'Paris', 'Berlin']
            })
            
            users_result = client.insert_data('users', users_df)
            assert users_result is not None
            assert users_result['operation'] == 'insert_data'
            
            # Check operation statistics
            users_stats = users_result['statistics']
            assert users_stats['rows_newly_inserted'] == 5
            assert users_stats['duplicates_found'] == 0
            
            # Insert orders data
            orders_df = pl.DataFrame({
                'order_id': [101, 102, 103, 104, 105, 106],
                'user_id': [1, 2, 1, 3, 2, 4],
                'product_id': [201, 202, 203, 201, 204, 202],
                'quantity': [2, 1, 3, 1, 2, 1],
                'total_amount': [29.98, 15.99, 44.97, 14.99, 31.98, 15.99]
            })
            
            orders_result = client.insert_data('orders', orders_df)
            assert orders_result is not None
            assert orders_result['operation'] == 'insert_data'
            
            # Insert products data
            products_df = pl.DataFrame({
                'product_id': [201, 202, 203, 204, 205],
                'name': ['Widget A', 'Widget B', 'Widget C', 'Widget D', 'Widget E'],
                'price': [14.99, 15.99, 14.99, 15.99, 19.99],
                'category': ['Electronics', 'Electronics', 'Home', 'Electronics', 'Home']
            })
            
            products_result = client.insert_data('products', products_df)
            assert products_result is not None
            assert products_result['operation'] == 'insert_data'
            
            # Test duplicate detection
            duplicate_users = pl.DataFrame({
                'user_id': [1, 6],  # user_id 1 is duplicate, 6 is new
                'email': ['user1@test.com', 'user6@test.com'],  # email for user 1 is duplicate
                'name': ['Alice Updated', 'Frank'],
                'age': [26, 40],
                'city': ['Boston', 'Madrid']
            })
            
            duplicate_result = client.insert_data('users', duplicate_users)
            assert duplicate_result is not None
            assert duplicate_result['operation'] == 'insert_data'
            
            # Check operation statistics for duplicate detection
            duplicate_stats = duplicate_result['statistics']
            assert duplicate_stats['rows_newly_inserted'] == 1  # Only user 6 should be inserted
            assert duplicate_stats['duplicates_found'] == 1  # User 1 should be detected as duplicate
            
            # Verify data integrity
            all_users = client.read_table('users')
            assert len(all_users) == 6  # 5 original + 1 new
            
            all_orders = client.read_table('orders')
            assert len(all_orders) == 6
            
            all_products = client.read_table('products')
            assert len(all_products) == 5
            
            # Test table info (replaces get_table_statistics)
            users_info = client.get_table_info('users')
            assert isinstance(users_info, dict)
            
            orders_info = client.get_table_info('orders')
            assert isinstance(orders_info, dict)
            
            # Test operation history
            users_history = client.get_operation_history('users')
            assert len(users_history) == 2  # Two insert operations
            
            orders_history = client.get_operation_history('orders')
            assert len(orders_history) == 1  # One insert operation
            
            # Test config persistence
            config.save()
            
            # Create new client and verify it can load existing data
            new_config = DBConfig(config_path, data_path)
            new_config.reload()
            new_client = DBClient(new_config)
            
            # Verify tables are still accessible
            assert 'users' in new_config.tables
            assert 'orders' in new_config.tables
            assert 'products' in new_config.tables
            
            # Verify data is still readable
            reloaded_users = new_client.read_table('users')
            assert len(reloaded_users) == 6
            
            reloaded_orders = new_client.read_table('orders')
            assert len(reloaded_orders) == 6
    
    def test_large_dataset_performance(self):
        """Test performance with larger datasets"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.json")
            data_path = os.path.join(temp_dir, "data")
            
            config = DBConfig(config_path, data_path)
            client = DBClient(config)
            
            # Add table for large dataset
            client.add_table('large_table', {'deduplication_columns': ['id']})
            
            # Create larger dataset (1000 rows)
            large_df = pl.DataFrame({
                'id': range(1, 1001),
                'value': [f"value_{i}" for i in range(1, 1001)],
                'category': [f"cat_{i % 10}" for i in range(1, 1001)],
                'score': [i * 0.1 for i in range(1, 1001)]
            })
            
            # Insert data
            result = client.insert_data('large_table', large_df)
            assert result is not None
            assert result['operation'] == 'insert_data'
            
            # Read back and verify
            read_data = client.read_table('large_table')
            assert len(read_data) == 1000
            
            # Test duplicate detection with large dataset
            duplicate_df = pl.DataFrame({
                'id': range(500, 1500),  # 500 duplicates, 500 new
                'value': [f"updated_value_{i}" for i in range(500, 1500)],
                'category': [f"updated_cat_{i % 10}" for i in range(500, 1500)],
                'score': [i * 0.2 for i in range(500, 1500)]
            })
            
            duplicate_result = client.insert_data('large_table', duplicate_df)
            assert duplicate_result is not None
            assert duplicate_result['operation'] == 'insert_data'
            
            # Check operation statistics
            second_op_stats = duplicate_result['statistics']
            assert second_op_stats['rows_newly_inserted'] == 499  # Only new IDs (1001-1499)
            assert second_op_stats['duplicates_found'] == 501  # Overlapping IDs (500-1000)
            
            # Verify final count
            final_data = client.read_table('large_table')
            assert len(final_data) == 1499  # 1000 original + 499 new 
    
    def test_error_recovery_and_validation(self):
        """Test error handling and recovery scenarios"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.json")
            data_path = os.path.join(temp_dir, "data")
            
            config = DBConfig(config_path, data_path)
            client = DBClient(config)
            
            # Test adding table with invalid configuration
            with pytest.raises(ValueError):
                client.add_table('invalid_table', {})  # Missing deduplication_columns
            
            # Add valid table
            client.add_table('test_table', {'deduplication_columns': ['id']})
            
            # Test inserting data to non-existent table
            test_df = pl.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
            
            with pytest.raises(ValueError):
                client.insert_data('nonexistent_table', test_df)
            
            # Test reading from table with no data
            with pytest.raises(FileNotFoundError):
                client.read_table('test_table')
            
            # Insert valid data
            client.insert_data('test_table', test_df)
            
            # Verify data was inserted correctly
            data = client.read_table('test_table')
            assert len(data) == 2
            
            # Test config save/reload cycle
            config.save()
            
            # Manually corrupt config file
            with open(config_path, 'w') as f:
                f.write("invalid json")
            
            # Test reload with corrupted config
            with pytest.raises(ValueError):
                new_config = DBConfig(config_path, data_path)
            
            # Restore valid config
            config.save()
            
            # Test successful reload
            new_config = DBConfig(config_path, data_path)
            new_config.reload()
            assert 'test_table' in new_config.tables 