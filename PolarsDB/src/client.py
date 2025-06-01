from os import path
from polars import read_csv, scan_csv, DataFrame, col
from typing import TYPE_CHECKING, List, Dict, Any, Optional
from datetime import datetime as dt

from .history_manager import OperationHistoryManager
from .statistics_calculator import StatisticsCalculator
from .query_executor import QueryExecutor
from .data_validator import DataValidator

if TYPE_CHECKING:
    from .config import DBConfig

class DBClient:
    """
    Professional database client with modular architecture
    
    This client provides a clean interface for database operations while delegating
    specialized functionality to dedicated components for better maintainability.
    """
    
    def __init__(self, config: "DBConfig"):
        self.config: "DBConfig" = config
        self.add_table = config.add_table
        self.remove_table = config.remove_table
        self.update_table = config.update_table
        
        # Initialize specialized components
        self.history_manager = OperationHistoryManager()
        self.query_executor = QueryExecutor(config, self.read_table)
        self.stats_calculator = StatisticsCalculator()
        self.data_validator = DataValidator()
    
    def read_table(self, table_name: str) -> DataFrame:
        """Read a table from disk into memory"""
        self.data_validator.validate_table_exists(table_name, list(self.config.tables.keys()))
        return read_csv(
            path.join(self.config.data_path, f"{table_name}.csv"), 
            infer_schema_length=5000, 
            infer_schema=True, 
            try_parse_dates=True
        )
    
    def scan_table(self, table_name: str):
        """Create a lazy scan of a table for efficient processing"""
        self.data_validator.validate_table_exists(table_name, list(self.config.tables.keys()))
        return scan_csv(
            path.join(self.config.data_path, f"{table_name}.csv"), 
            try_parse_dates=True, 
            infer_schema_length=5000, 
            infer_schema=True
        )
    
    def query(self, query_str: str, params: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Execute a SQL query using Polars SQLContext, loading only referenced tables
        
        This method provides a SQL interface to query the database.
        Only tables referenced in the SQL query will be loaded into memory.
        
        Parameters:
        -----------
        query_str : str
            SQL query string to execute (e.g., "SELECT * FROM customers WHERE age > 25")
        params : Dict[str, Any], optional
            Parameters to substitute in the query (for future use)
            
        Returns:
        --------
        DataFrame
            Result of the SQL query
        
        Examples:
        ---------
        # Simple SELECT
        client.query("SELECT * FROM customers WHERE age > 25")
        
        # JOIN multiple tables
        client.query('''
            SELECT c.name, o.order_id, o.total
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE c.age > 30
        ''')
        
        # Common Table Expression (CTE)
        client.query('''
            WITH high_value_customers AS (
                SELECT customer_id, name, total_spent
                FROM customers 
                WHERE total_spent > 1000
            )
            SELECT * FROM high_value_customers
            ORDER BY total_spent DESC
        ''')
        """
        
        # Validate SQL query
        if not self.query_executor.validate_query(query_str):
            raise ValueError("SQL query contains potentially dangerous operations or syntax errors")
        
        try:
            # Execute SQL query using the query executor
            result, execution_time, tables_used = self.query_executor.execute_query(query_str, params)
            
            # Calculate statistics
            namespace = {'pl': __import__('polars')}
            for table_name in tables_used:
                try:
                    namespace[table_name] = self.read_table(table_name)
                except:
                    pass
            
            statistics = self.stats_calculator.calculate_query_statistics(
                result, execution_time, tables_used, namespace
            )
            
            # Create operation record
            operation_record = {
                "operation": "query",
                "args": {
                    "query": query_str,
                    "params": str(params) if params else None,
                    "tables_used": tables_used
                },
                "statistics": statistics,
                "summary": self.stats_calculator.create_operation_summary(
                    "query", statistics
                ),
                "timestamp": dt.now()
            }
            
            # Record operation for all tables involved
            self.history_manager.record_operation_multiple_tables(tables_used, operation_record)
            
            return result
            
        except Exception as e:
            raise ValueError(f"Error executing SQL query: {str(e)}")
    
    def insert_data(self, table_name: str, df: DataFrame):
        """Insert data into a table, respecting the deduplication columns, updating existing rows and appending new rows"""
        
        # Validate inputs
        self.data_validator.validate_table_exists(table_name, list(self.config.tables.keys()))
        self.data_validator.validate_dataframe(df)
        
        table_config = self.config.get_table_config(table_name)
        deduplication_columns = table_config["deduplication_columns"]
        
        # Validate deduplication columns
        self.data_validator.validate_deduplication_columns(df, deduplication_columns)
        
        file_path = path.join(self.config.data_path, f"{table_name}.csv")
        
        existing_df = None
        if path.exists(file_path):
            existing_df = self.read_table(table_name)
            
            # Anti-join to keep only rows from existing_df that don't match keys in the new df
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
        
        # Calculate detailed statistics
        statistics = self.stats_calculator.calculate_insert_statistics(
            df, existing_df, deduplication_columns, result_df
        )
        
        # Create operation record
        operation_record = {
            "operation": "insert_data",
            "args": {
                "table_name": table_name, 
                "input_data_shape": df.shape,
                "deduplication_columns": deduplication_columns
            },
            "statistics": statistics,
            "summary": self.stats_calculator.create_operation_summary(
                "insert_data", statistics
            ),
            "timestamp": dt.now()
        }
        
        # Record operation
        self.history_manager.record_operation(table_name, operation_record)
        
        # Return the operation record for immediate feedback
        return operation_record
    
    def delete_data_by_date(self, table_name: str, date_column: str, min_date: dt, max_date: dt):
        """Delete data within a date range"""
        
        # Validate inputs
        self.data_validator.validate_table_exists(table_name, list(self.config.tables.keys()))
        
        df = self.read_table(table_name)
        self.data_validator.validate_column_exists(df, date_column)
        
        # Count rows that will be deleted
        rows_to_delete = df.filter(col(date_column).is_between(min_date, max_date))
        deleted_count = rows_to_delete.height
        
        df_filtered = df.filter(~col(date_column).is_between(min_date, max_date))
        df_filtered.write_csv(path.join(self.config.data_path, f"{table_name}.csv"))
        
        # Calculate statistics
        statistics = self.stats_calculator.calculate_delete_statistics(df, df_filtered, deleted_count)
        
        # Create operation record
        operation_record = {
            "operation": "delete_data_by_date",
            "args": {
                "table_name": table_name, 
                "date_column": date_column, 
                "min_date": min_date, 
                "max_date": max_date
            },
            "statistics": statistics,
            "summary": self.stats_calculator.create_operation_summary(
                "delete_data_by_date", statistics, min_date=min_date, max_date=max_date
            ),
            "timestamp": dt.now()
        }
        
        # Record operation
        self.history_manager.record_operation(table_name, operation_record)
        
        # Return the operation record for immediate feedback
        return operation_record
    
    def delete_data_by_key(self, table_name: str, key_column: str, key_value: str):
        """Delete data by key value"""
        
        # Validate inputs
        self.data_validator.validate_table_exists(table_name, list(self.config.tables.keys()))
        
        df = self.read_table(table_name)
        self.data_validator.validate_column_exists(df, key_column)
        
        # Count rows that will be deleted
        rows_to_delete = df.filter(col(key_column) == key_value)
        deleted_count = rows_to_delete.height
        
        df_filtered = df.filter(col(key_column) != key_value)
        df_filtered.write_csv(path.join(self.config.data_path, f"{table_name}.csv"))
        
        # Calculate statistics
        statistics = self.stats_calculator.calculate_delete_statistics(df, df_filtered, deleted_count)
        
        # Create operation record
        operation_record = {
            "operation": "delete_data_by_key",
            "args": {
                "table_name": table_name, 
                "key_column": key_column, 
                "key_value": key_value
            },
            "statistics": statistics,
            "summary": self.stats_calculator.create_operation_summary(
                "delete_data_by_key", statistics, key_column=key_column, key_value=key_value
            ),
            "timestamp": dt.now()
        }
        
        # Record operation
        self.history_manager.record_operation(table_name, operation_record)
        
        # Return the operation record for immediate feedback
        return operation_record
    
    # History management methods - delegate to history manager
    def get_operation_history(self, table_name: str) -> List[Dict[str, Any]]:
        """Get operation history for a specific table"""
        return self.history_manager.get_history(table_name)
    
    def get_all_operation_history(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get complete operation history"""
        return self.history_manager.get_all_history()
    
    def get_operation_summary(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get a summary of operations performed on a table or all tables"""
        return self.history_manager.get_operation_summary(table_name)
    
    def get_recent_operations(self, table_name: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most recent operations for a table or all tables"""
        return self.history_manager.get_recent_operations(table_name, limit)
    
    def print_operation_history(self, table_name: Optional[str] = None, detailed: bool = False):
        """Print a formatted view of operation history"""
        self.history_manager.print_operation_history(table_name, detailed)
    
    # Query executor methods - delegate to query executor
    def get_available_tables(self) -> List[str]:
        """Get list of available tables for queries"""
        return self.query_executor.get_available_tables()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a specific table"""
        return self.query_executor.get_table_info(table_name)
    
    # Data validator methods - delegate to data validator
    def get_dataframe_info(self, df: DataFrame) -> Dict[str, Any]:
        """Get comprehensive information about a dataframe"""
        return self.data_validator.get_dataframe_info(df)
    
    def explain_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Explain what tables would be loaded for a given SQL query without executing it
        
        This is useful for understanding the impact of a query before running it.
        
        Parameters:
        -----------
        sql_query : str
            SQL query string to analyze
            
        Returns:
        --------
        Dict[str, Any]
            Information about the query plan including tables to be loaded
        """
        return self.query_executor.explain_query(sql_query)
    
    def show_tables(self) -> DataFrame:
        """
        Show all available tables in the database with their basic information
        
        Returns:
        --------
        DataFrame
            Information about all tables including existence, row count, columns
        """
        return self.query_executor.show_available_tables()

