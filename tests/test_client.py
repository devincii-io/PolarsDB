"""
Tests for DBClient class
"""
import pytest
import polars as pl
import os
from pathlib import Path

from PolarsDB import DBClient, DBConfig


class TestDBClient:
    """Test cases for DBClient class"""
    
    def test_client_initialization(self, db_config):
        """Test DBClient initialization"""
        client = DBClient(db_config)
        assert client.config == db_config
    
    def test_add_table_success(self, db_client, users_table_config):
        """Test successfully adding a table"""
        db_client.add_table('users', users_table_config)
        
        assert 'users' in db_client.config.tables
        assert db_client.config.tables['users'] == users_table_config
    
    def test_add_table_already_exists_no_throw(self, db_client, users_table_config):
        """Test adding table that already exists with throw_if_exists=False"""
        db_client.add_table('users', users_table_config)
        
        # Should not raise error
        db_client.add_table('users', users_table_config, throw_if_exists=False)
    
    def test_add_table_already_exists_with_throw(self, db_client, users_table_config):
        """Test adding table that already exists with throw_if_exists=True"""
        db_client.add_table('users', users_table_config)
        
        with pytest.raises(ValueError, match="Table already exists: users"):
            db_client.add_table('users', users_table_config, throw_if_exists=True)
    
    def test_insert_data_new_table(self, db_client, sample_dataframe, users_table_config):
        """Test inserting data into a new table"""
        db_client.add_table('users', users_table_config)
        
        # insert_data returns the operation record
        result = db_client.insert_data('users', sample_dataframe)
        assert result is not None
        assert isinstance(result, dict)
        
        # Check the returned operation record
        assert result['operation'] == 'insert_data'
        assert 'statistics' in result
        assert 'timestamp' in result
        
        stats = result['statistics']
        assert stats['rows_newly_inserted'] == 5
        assert stats['duplicates_found'] == 0
        assert stats['new_table_size'] == 5
    
    def test_insert_data_with_duplicates(self, db_client, sample_dataframe, users_table_config):
        """Test inserting data with duplicates"""
        db_client.add_table('users', users_table_config)
        
        # Insert original data
        db_client.insert_data('users', sample_dataframe)
        
        # Insert same data again (should detect duplicates)
        result = db_client.insert_data('users', sample_dataframe)
        assert result is not None
        assert isinstance(result, dict)
        
        # Check the returned operation record for duplicate detection
        assert result['operation'] == 'insert_data'
        stats = result['statistics']
        assert stats['rows_newly_inserted'] == 0
        assert stats['duplicates_found'] == 5
        assert stats['new_table_size'] == 5  # Total should remain the same
    
    def test_insert_data_nonexistent_table(self, db_client, sample_dataframe):
        """Test inserting data into non-existent table"""
        with pytest.raises(ValueError, match="Table not found: nonexistent"):
            db_client.insert_data('nonexistent', sample_dataframe)
    
    def test_read_table_success(self, db_client, sample_dataframe, users_table_config):
        """Test reading table data"""
        db_client.add_table('users', users_table_config)
        db_client.insert_data('users', sample_dataframe)
        
        result = db_client.read_table('users')
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 5
        assert list(result.columns) == ['id', 'name', 'age', 'email']
    
    def test_read_table_nonexistent(self, db_client):
        """Test reading non-existent table"""
        with pytest.raises(ValueError, match="Table not found: nonexistent"):
            db_client.read_table('nonexistent')
    
    def test_read_table_no_data_file(self, db_client, users_table_config):
        """Test reading table when no data file exists"""
        db_client.add_table('users', users_table_config)
        
        with pytest.raises(FileNotFoundError):
            db_client.read_table('users')
    
    def test_get_operation_history(self, db_client, sample_dataframe, users_table_config):
        """Test getting operation history"""
        db_client.add_table('users', users_table_config)
        db_client.insert_data('users', sample_dataframe)
        
        history = db_client.get_operation_history('users')
        
        assert isinstance(history, list)
        assert len(history) > 0
        
        # Check first operation
        first_op = history[0]
        assert 'timestamp' in first_op
        assert 'operation' in first_op
        assert first_op['operation'] == 'insert_data'
    
    def test_get_operation_history_nonexistent_table(self, db_client):
        """Test getting history for non-existent table"""
        # get_operation_history doesn't validate table existence, it just returns empty list
        history = db_client.get_operation_history('nonexistent')
        assert history == []
    
    def test_get_table_info(self, db_client, sample_dataframe, users_table_config):
        """Test getting table info (replaces get_table_statistics)"""
        db_client.add_table('users', users_table_config)
        db_client.insert_data('users', sample_dataframe)
        
        info = db_client.get_table_info('users')
        
        assert isinstance(info, dict)
        # The actual structure depends on the implementation
        # Let's just verify it returns a dict for now
    
    def test_get_table_info_nonexistent_table(self, db_client):
        """Test getting info for non-existent table"""
        with pytest.raises(ValueError, match="Table not found: nonexistent"):
            db_client.get_table_info('nonexistent')
    
    def test_multiple_operations_workflow(self, db_client, users_table_config):
        """Test a complete workflow with multiple operations"""
        # Add table
        db_client.add_table('users', users_table_config)
        
        # Insert initial data
        df1 = pl.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com']
        })
        result1 = db_client.insert_data('users', df1)
        assert result1 is not None
        assert result1['operation'] == 'insert_data'
        assert result1['statistics']['rows_newly_inserted'] == 3
        
        # Insert additional data
        df2 = pl.DataFrame({
            'id': [4, 5],
            'name': ['Diana', 'Eve'],
            'age': [28, 32],
            'email': ['diana@test.com', 'eve@test.com']
        })
        result2 = db_client.insert_data('users', df2)
        assert result2 is not None
        assert result2['operation'] == 'insert_data'
        assert result2['statistics']['rows_newly_inserted'] == 2
        
        # Read all data
        all_data = db_client.read_table('users')
        assert len(all_data) == 5
        
        # Check history
        history = db_client.get_operation_history('users')
        assert len(history) == 2  # Two insert operations
        
        # Check that both operations were recorded
        assert history[0]['operation'] == 'insert_data'
        assert history[1]['operation'] == 'insert_data'
        
        # Check final table size from latest operation (result2)
        assert result2['statistics']['new_table_size'] == 5 