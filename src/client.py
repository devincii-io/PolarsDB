from os import path
from polars import read_csv, scan_csv, DataFrame, col
from typing import TYPE_CHECKING, List, Dict, Any, Union, Optional
from datetime import datetime as dt

if TYPE_CHECKING:
    from src.config import DBConfig

class DBClient:
    def __init__(self, config: "DBConfig"):
        self.config = config
        self.operation_history = {} # {table_name: [operation_history]}, operation_history = {operation_name: [operation_args], old_size: int, new_size: int}
    
    def read_table(self, table_name: str):
        if table_name not in self.config.tables:
            raise ValueError(f"Table not found: {table_name}")
        return read_csv(path.join(self.config.data_path, f"{table_name}.csv"), infer_schema_length=5000, infer_schema=True, try_parse_dates=True)
    
    def scan_table(self, table_name: str):
        if table_name not in self.config.tables:
            raise ValueError(f"Table not found: {table_name}")
        return scan_csv(path.join(self.config.data_path, f"{table_name}.csv"), try_parse_dates=True, infer_schema_length=5000, infer_schema=True)
    
    def query(self, query_str: str, params: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Execute a query across all tables in the database using Polars expressions
        
        This method provides a simple SQL-like interface to query the database.
        The query string can reference any table by name and use Polars expressions.
        
        Parameters:
        -----------
        query_str : str
            A string containing Polars expressions to execute
            Example: "customers.filter(pl.col('total_spent') > 1000).join(orders, on='customer_id')"
        params : Dict[str, Any], optional
            Parameters to substitute in the query
            
        Returns:
        --------
        DataFrame
            Result of the query
        
        Examples:
        ---------
        # Simple filter
        client.query("customers.filter(pl.col('total_spent') > 1000)")
        
        # Join tables
        client.query("customers.join(orders, on='customer_id').select(['name', 'order_id'])")
        
        # Parameterized query
        client.query("customers.filter(pl.col('total_spent') > threshold)", {"threshold": 1000})
        """
        import polars as pl
        
        # Create a local namespace with all tables
        namespace = {'pl': pl, 'col': pl.col}
        
        # Load all tables into the namespace
        for table_name in self.config.tables:
            try:
                namespace[table_name] = self.read_table(table_name)
            except Exception as e:
                # If table file doesn't exist, skip it
                pass
        
        # Add parameters to namespace if provided
        if params:
            namespace.update(params)
        
        # Execute the query
        try:
            result = eval(query_str, {"__builtins__": {}}, namespace)
            
            # Ensure result is a DataFrame
            if not isinstance(result, DataFrame):
                raise ValueError("Query must return a DataFrame")
            
            # Record operation in history
            tables_used = [t for t in self.config.tables if t in query_str]
            operation_record = {
                "operation": "query",
                "args": {
                    "query": query_str,
                    "params": str(params) if params else None,
                    "tables_used": tables_used,
                    "result_shape": result.shape
                },
                "timestamp": dt.now()
            }
            
            # Add to the operation history of all tables involved
            for table in tables_used:
                if table not in self.operation_history:
                    self.operation_history[table] = []
                self.operation_history[table].append(operation_record)
            
            return result
            
        except Exception as e:
            raise ValueError(f"Error executing query: {str(e)}")
    
    def insert_data(self, table_name: str, df: DataFrame):
        """Insert data into a table, respecting the deduplication columns, updating the already existing rows with the new data, and appending the new rows to the end of the file"""
        table_config = self.config.get_table_config(table_name)
        deduplication_columns = table_config["deduplication_columns"]
        #check if deduplication columns are in the dataframe
        if not all(col in df.columns for col in deduplication_columns):
            raise ValueError(f"Deduplication columns not found in dataframe: {deduplication_columns}")
        
        file_path = path.join(self.config.data_path, f"{table_name}.csv")
        
        old_size = 0
        if path.exists(file_path):
            existing_df = self.read_table(table_name)
            old_size = existing_df.height
            
            # Anti-join to keep only rows from existing_df that don't match keys in the new df
            # (remove rows that will be updated)
            preserved_rows = existing_df.join(
                df.select(deduplication_columns),
                on=deduplication_columns,
                how="anti"
            )
            
            # Stack the preserved rows with the new data
            result_df = preserved_rows.vstack(df)
        else:
            # If the table doesn't exist yet, the result is just the new data
            result_df = df
            
        # Write the complete result (overwrites the file)
        result_df.write_csv(file_path)
        
        # Record operation in history
        new_size = result_df.height
        operation_record = {
            "operation": "insert_data",
            "args": {"table_name": table_name, "data_shape": df.shape},
            "old_size": old_size,
            "new_size": new_size,
            "timestamp": dt.now()
        }
        
        if table_name not in self.operation_history:
            self.operation_history[table_name] = []
        self.operation_history[table_name].append(operation_record)
    
    def delete_data_by_date(self, table_name: str, date_column: str, min_date: dt, max_date: dt):
        df = self.read_table(table_name)
        old_size = df.height
        
        df_filtered = df.filter(~col(date_column).is_between(min_date, max_date))
        df_filtered.write_csv(path.join(self.config.data_path, f"{table_name}.csv"))
        
        # Record operation in history
        new_size = df_filtered.height
        operation_record = {
            "operation": "delete_data_by_date",
            "args": {"table_name": table_name, "date_column": date_column, "min_date": min_date, "max_date": max_date},
            "old_size": old_size,
            "new_size": new_size,
            "timestamp": dt.now()
        }
        
        if table_name not in self.operation_history:
            self.operation_history[table_name] = []
        self.operation_history[table_name].append(operation_record)
    
    def delete_data_by_key(self, table_name: str, key_column: str, key_value: str):
        df = self.read_table(table_name)
        old_size = df.height
        
        df_filtered = df.filter(col(key_column) != key_value)
        df_filtered.write_csv(path.join(self.config.data_path, f"{table_name}.csv"))
        
        # Record operation in history
        new_size = df_filtered.height
        operation_record = {
            "operation": "delete_data_by_key",
            "args": {"table_name": table_name, "key_column": key_column, "key_value": key_value},
            "old_size": old_size,
            "new_size": new_size,
            "timestamp": dt.now()
        }
        
        if table_name not in self.operation_history:
            self.operation_history[table_name] = []
        self.operation_history[table_name].append(operation_record)
        
    def get_operation_history(self, table_name: str):
        if table_name not in self.operation_history:
            return []
        return self.operation_history[table_name]
    
    def get_all_operation_history(self):
        return self.operation_history

