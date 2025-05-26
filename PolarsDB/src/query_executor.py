from polars import DataFrame
from typing import Dict, Any, Optional, List, TYPE_CHECKING
from datetime import datetime as dt

if TYPE_CHECKING:
    from .config import DBConfig


class QueryExecutor:
    """Handles query execution and namespace management"""
    
    def __init__(self, config: "DBConfig", table_reader_func):
        self.config = config
        self.read_table = table_reader_func
    
    def execute_query(self, query_str: str, params: Optional[Dict[str, Any]] = None) -> tuple[DataFrame, float, List[str]]:
        """
        Execute a query and return the result, execution time, and tables used
        
        Returns:
            tuple: (result_dataframe, execution_time_seconds, tables_used)
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
        
        # Execute the query with timing
        start_time = dt.now()
        result = eval(query_str, {"__builtins__": {}}, namespace)
        execution_time = (dt.now() - start_time).total_seconds()
        
        # Ensure result is a DataFrame
        if not isinstance(result, DataFrame):
            raise ValueError("Query must return a DataFrame")
        
        # Determine which tables were used
        tables_used = [t for t in self.config.tables if t in query_str]
        
        return result, execution_time, tables_used
    
    def validate_query(self, query_str: str) -> bool:
        """
        Basic validation of query string
        
        Returns:
            bool: True if query appears valid, False otherwise
        """
        # Check for dangerous operations
        dangerous_keywords = ['import', 'exec', 'eval', '__', 'open', 'file']
        query_lower = query_str.lower()
        
        for keyword in dangerous_keywords:
            if keyword in query_lower:
                return False
        
        return True
    
    def get_available_tables(self) -> List[str]:
        """Get list of available tables for queries"""
        return list(self.config.tables.keys())
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a specific table"""
        if table_name not in self.config.tables:
            raise ValueError(f"Table not found: {table_name}")
        
        try:
            df = self.read_table(table_name)
            return {
                "table_name": table_name,
                "shape": df.shape,
                "columns": df.columns,
                "dtypes": dict(zip(df.columns, [str(dtype) for dtype in df.dtypes]))
            }
        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e),
                "exists": False
            } 