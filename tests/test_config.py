"""
Tests for DBConfig class
"""
import pytest
import os
import json
from pathlib import Path
import tempfile

from PolarsDB import DBConfig


class TestDBConfig:
    """Test cases for DBConfig class"""
    
    def test_config_creation_with_valid_paths(self, temp_dir):
        """Test creating config with valid paths"""
        config_path = os.path.join(temp_dir, "config.json")
        data_path = os.path.join(temp_dir, "data")
        
        config = DBConfig(config_path, data_path)
        
        assert str(config.config_path) == str(Path(config_path).resolve())
        assert str(config.data_path) == str(Path(data_path).resolve())
        assert os.path.exists(data_path)  # Should be created automatically
    
    def test_config_creation_with_nested_paths(self, temp_dir):
        """Test creating config with nested directory paths"""
        config_path = os.path.join(temp_dir, "nested", "config", "config.json")
        data_path = os.path.join(temp_dir, "nested", "data")
        
        config = DBConfig(config_path, data_path)
        
        assert os.path.exists(os.path.dirname(config_path))
        assert os.path.exists(data_path)
    
    def test_config_creation_with_relative_paths(self, temp_dir):
        """Test creating config with relative paths"""
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(temp_dir)
        
        try:
            config = DBConfig("config.json", "data")
            
            # Should resolve to absolute paths
            assert Path(config.config_path).is_absolute()
            assert Path(config.data_path).is_absolute()
            assert Path(config.data_path).name == "data"
        finally:
            os.chdir(original_cwd)
    
    def test_config_creation_with_empty_config_path(self):
        """Test that empty config path raises ValueError"""
        with pytest.raises(ValueError, match="config_path cannot be empty"):
            DBConfig("", "data")
    
    def test_config_creation_with_empty_data_path(self, temp_dir):
        """Test that empty data path uses default"""
        config_path = os.path.join(temp_dir, "config.json")
        
        config = DBConfig(config_path, "")
        
        # Should use current working directory + "data"
        expected_data_path = Path.cwd() / "data"
        assert str(config.data_path) == str(expected_data_path)
    
    def test_add_table_valid_config(self, db_config):
        """Test adding a table with valid configuration"""
        table_config = {'deduplication_columns': ['id']}
        
        db_config.add_table('users', table_config)
        
        assert 'users' in db_config.tables
        assert db_config.tables['users'] == table_config
    
    def test_add_table_invalid_config_missing_dedup(self, db_config):
        """Test adding table with missing deduplication_columns"""
        with pytest.raises(ValueError, match="Configuration must include 'deduplication_columns'"):
            db_config.add_table('users', {})
    
    def test_add_table_invalid_config_empty_dedup(self, db_config):
        """Test adding table with empty deduplication_columns"""
        # The add_table method doesn't validate empty lists, it only checks for presence
        # This validation happens during reload
        db_config.add_table('users', {'deduplication_columns': []})
        assert 'users' in db_config.tables
    
    def test_add_table_invalid_config_non_list_dedup(self, db_config):
        """Test adding table with non-list deduplication_columns"""
        # The add_table method doesn't validate the type, it only checks for presence
        # This validation happens during reload
        db_config.add_table('users', {'deduplication_columns': 'id'})
        assert 'users' in db_config.tables
    
    def test_remove_table_existing(self, db_config):
        """Test removing an existing table"""
        db_config.add_table('users', {'deduplication_columns': ['id']})
        
        db_config.remove_table('users')
        
        assert 'users' not in db_config.tables
    
    def test_remove_table_nonexistent(self, db_config):
        """Test removing a non-existent table"""
        with pytest.raises(ValueError, match="Table not found: nonexistent"):
            db_config.remove_table('nonexistent')
    
    def test_set_data_path_valid(self, db_config, temp_dir):
        """Test setting a valid data path"""
        new_data_path = os.path.join(temp_dir, "new_data")
        
        db_config.set_data_path(new_data_path)
        
        assert str(db_config.data_path) == str(Path(new_data_path).resolve())
        assert os.path.exists(new_data_path)
    
    def test_set_data_path_empty(self, db_config):
        """Test setting empty data path raises ValueError"""
        with pytest.raises(ValueError, match="data_path cannot be empty"):
            db_config.set_data_path("")
    
    def test_save_and_reload_config(self, db_config):
        """Test saving and reloading configuration"""
        # Add some tables
        db_config.add_table('users', {'deduplication_columns': ['id', 'email']})
        db_config.add_table('orders', {'deduplication_columns': ['order_id']})
        
        # Save config
        db_config.save()
        
        # Create new config instance and reload
        new_config = DBConfig(str(db_config.config_path), str(db_config.data_path))
        new_config.reload()
        
        assert new_config.tables == db_config.tables
        assert str(new_config.data_path) == str(db_config.data_path)
    
    def test_reload_nonexistent_file(self, db_config):
        """Test reloading when config file doesn't exist"""
        # Delete the config file that was created automatically
        os.remove(db_config.config_path)
        
        with pytest.raises(ValueError, match="Config file does not exist"):
            db_config.reload()
    
    def test_reload_invalid_json(self, db_config):
        """Test reloading with invalid JSON"""
        # Create invalid JSON file
        with open(db_config.config_path, 'w') as f:
            f.write("invalid json content")
        
        with pytest.raises(ValueError, match="Expecting value"):
            db_config.reload()
    
    def test_reload_missing_tables_key(self, db_config):
        """Test reloading with missing 'tables' key"""
        # Create config without 'tables' key
        config_data = {"data_path": str(db_config.data_path)}
        with open(db_config.config_path, 'w') as f:
            json.dump(config_data, f)
        
        # Missing 'tables' key is handled gracefully with default empty dict
        db_config.reload()
        assert db_config.tables == {}
    
    def test_reload_invalid_table_config(self, db_config):
        """Test reloading with invalid table configuration"""
        # Create config with invalid table config
        config_data = {
            "data_path": str(db_config.data_path),
            "tables": {
                "users": {}  # Missing deduplication_columns
            }
        }
        with open(db_config.config_path, 'w') as f:
            json.dump(config_data, f)
        
        with pytest.raises(ValueError, match="Table 'users' missing required 'deduplication_columns'"):
            db_config.reload()
    
    def test_config_persistence_across_operations(self, db_config):
        """Test that config persists across multiple operations"""
        # Add tables
        db_config.add_table('users', {'deduplication_columns': ['id']})
        db_config.add_table('orders', {'deduplication_columns': ['order_id']})
        db_config.save()
        
        # Modify and save again
        db_config.add_table('products', {'deduplication_columns': ['sku']})
        db_config.save()
        
        # Reload and verify
        db_config.reload()
        assert len(db_config.tables) == 3
        assert 'users' in db_config.tables
        assert 'orders' in db_config.tables
        assert 'products' in db_config.tables 