from json import dump, load
from os import path
from typing import Dict, Any

"""
{
    "tables": [
        {
            "name": "table_name",
            "deduplication_columns": ["column_name", "column_name", "column_name"]
        },
        {
            "name": "table_name",
            "deduplication_columns": ["column_name", "column_name", "column_name"]
        }
    ],
    "data_path": "data_path"
}
"""

class DBConfig:
    """Configuration class for the PolarsDB database"""
    
    def __init__(self, config_path: str, data_path: str = None, tables: Dict[str, Dict[str, Any]] = None):
        """
        Initialize the database configuration
        
        Args:
            config_path: Path to the configuration file
            data_path: Path where the CSV files will be stored
            tables: Dictionary of table configurations with the format:
                   {
                       "table_name": {
                           "deduplication_columns": ["col1", "col2"]
                       },
                       ...
                   }
        """
        self.config_path = config_path
        self.data_path = data_path if data_path else ""
        self.tables = tables if tables else {}
        
        # Create config file if it doesn't exist
        if not path.exists(config_path):
            self.config = {
                "tables": self.tables,
                "data_path": self.data_path
            }
            self.save()
        else:
            # If tables were passed in, use them instead of loading from file
            if tables:
                self.config = {
                    "tables": self.tables,
                    "data_path": self.data_path
                }
                self.save()
            else:
                self.reload()
    
    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get the configuration for a specific table"""
        if table_name not in self.tables:
            raise ValueError(f"Table not found: {table_name}")
        return self.tables[table_name]
    
    def add_table(self, table_name: str, config: Dict[str, Any]):
        """Add a new table configuration"""
        if table_name in self.tables:
            raise ValueError(f"Table already exists: {table_name}")
        
        # Validate configuration
        if "deduplication_columns" not in config:
            raise ValueError("Configuration must include 'deduplication_columns'")
        
        self.tables[table_name] = config
        self.save()
    
    def update_table(self, table_name: str, config: Dict[str, Any]):
        """Update an existing table configuration"""
        if table_name not in self.tables:
            raise ValueError(f"Table not found: {table_name}")
        self.tables[table_name].update(config)
        self.save()
    
    def remove_table(self, table_name: str):
        """Remove a table configuration"""
        if table_name not in self.tables:
            raise ValueError(f"Table not found: {table_name}")
        del self.tables[table_name]
        self.save()
    
    def set_data_path(self, data_path: str):
        """Set the data path"""
        self.data_path = data_path
        self.save()
    
    def save(self):
        """Save the configuration to file"""
        # Convert tables dictionary to the format expected in the config file
        self.config["tables"] = self.tables
        self.config["data_path"] = self.data_path
        
        with open(self.config_path, "w") as f:
            dump(self.config, f)
    
    def reload(self):
        """Reload the configuration from file"""
        try:
            with open(self.config_path, "r") as f:
                self.config = load(f)
                
            # Get tables and data_path from the config
            self.tables = self.config["tables"]
            self.data_path = self.config["data_path"]
        except Exception as e:
            # If there's an error loading, create a new config
            self.config = {
                "tables": {},
                "data_path": self.data_path
            }
            self.save()
    
    def __repr__(self):
        return f"DBConfig(config_path={self.config_path}, tables={self.tables}, data_path={self.data_path})"

