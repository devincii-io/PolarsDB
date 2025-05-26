from json import dump, load
from os import path
from pathlib import Path
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
        # Validate and normalize paths
        if not config_path:
            raise ValueError("config_path cannot be empty")
        
        self.config_path = str(Path(config_path).resolve())
        self.data_path = str(Path(data_path).resolve()) if data_path else str(Path.cwd() / "data")
        self.tables = tables if tables else {}
        
        # Ensure config directory exists
        config_dir = Path(self.config_path).parent
        try:
            config_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot create config directory {config_dir}: {e}")
        
        # Ensure data directory exists
        try:
            Path(self.data_path).mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot create data directory {self.data_path}: {e}")
        
        # Create config file if it doesn't exist
        if not path.exists(self.config_path):
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
    
    def add_table(self, table_name: str, config: Dict[str, Any], throw_if_exists: bool = True):
        """Add a new table configuration to the database.
        
        This method adds a new table configuration to the database schema. The configuration
        must include deduplication columns which are used to identify unique rows and handle
        updates/inserts appropriately.
        
        Args:
            table_name: The name of the table to add
            config: Dictionary containing the table configuration with the format:
                   {
                       "deduplication_columns": ["col1", "col2", ...]  # Required
                   }
                   
        Raises:
            ValueError: If the table already exists or if the configuration is invalid
        """
        if table_name in self.tables:
            if throw_if_exists:
                raise ValueError(f"Table already exists: {table_name}")
            else:
                return
        # Validate configuration
        if "deduplication_columns" not in config:
            raise ValueError("Configuration must include 'deduplication_columns'")
        
        self.tables[table_name] = config
        self.save()
    
    def update_table(self, table_name: str, config: Dict[str, Any], throw_if_not_found: bool = True):
        """Update an existing table configuration"""
        if table_name not in self.tables:
            if throw_if_not_found:
                raise ValueError(f"Table not found: {table_name}")
            else:
                return
        self.tables[table_name].update(config)
        self.save()
    
    def remove_table(self, table_name: str, throw_if_not_found: bool = True):
        """Remove a table configuration"""
        if table_name not in self.tables:
            if throw_if_not_found:
                raise ValueError(f"Table not found: {table_name}")
            else:
                return
        del self.tables[table_name]
        self.save()
    
    def set_data_path(self, data_path: str):
        """Set the data path"""
        if not data_path:
            raise ValueError("data_path cannot be empty")
        
        # Normalize and validate the path
        normalized_path = str(Path(data_path).resolve())
        
        # Ensure the directory exists
        try:
            Path(normalized_path).mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot create data directory {normalized_path}: {e}")
        
        self.data_path = normalized_path
        self.save()
    
    def save(self):
        """Save the configuration to file"""
        # Convert tables dictionary to the format expected in the config file
        self.config = {
            "tables": self.tables,
            "data_path": self.data_path
        }
        
        try:
            with open(self.config_path, "w") as f:
                dump(self.config, f, indent=2)
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot write to config file {self.config_path}: {e}")
    
    def reload(self):
        """Reload the configuration from file"""
        if not path.exists(self.config_path):
            raise ValueError(f"Config file does not exist: {self.config_path}")
        
        try:
            with open(self.config_path, "r") as f:
                self.config = load(f)
                
            # Validate config structure
            if not isinstance(self.config, dict):
                raise ValueError("Config file must contain a JSON object")
            
            # Get tables and data_path from the config with defaults
            self.tables = self.config.get("tables", {})
            loaded_data_path = self.config.get("data_path", self.data_path)
            
            # Validate and normalize data path
            if loaded_data_path:
                normalized_path = str(Path(loaded_data_path).resolve())
                try:
                    Path(normalized_path).mkdir(parents=True, exist_ok=True)
                    self.data_path = normalized_path
                except (OSError, PermissionError) as e:
                    raise ValueError(f"Cannot access data directory {normalized_path}: {e}")
            
            # Validate tables structure
            if not isinstance(self.tables, dict):
                raise ValueError("Tables configuration must be a dictionary")
            
            for table_name, table_config in self.tables.items():
                if not isinstance(table_config, dict):
                    raise ValueError(f"Table '{table_name}' configuration must be a dictionary")
                if "deduplication_columns" not in table_config:
                    raise ValueError(f"Table '{table_name}' missing required 'deduplication_columns'")
                if not isinstance(table_config["deduplication_columns"], list):
                    raise ValueError(f"Table '{table_name}' deduplication_columns must be a list")
                    
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot read config file {self.config_path}: {e}")
        except ValueError as e:
            # Re-raise validation errors
            raise e
        except Exception as e:
            # Handle JSON parsing errors and other unexpected errors
            raise ValueError(f"Invalid config file format {self.config_path}: {e}")
    
    def __repr__(self):
        return f"DBConfig(config_path={self.config_path}, tables={self.tables}, data_path={self.data_path})"

