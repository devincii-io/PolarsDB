from polars import DataFrame
from typing import List, Dict, Any, Optional


class DataValidator:
    """Utility class for data validation operations"""
    
    @staticmethod
    def validate_deduplication_columns(df: DataFrame, deduplication_columns: List[str]) -> bool:
        """
        Validate that deduplication columns exist in the dataframe
        
        Args:
            df: DataFrame to validate
            deduplication_columns: List of column names that should exist
            
        Returns:
            bool: True if all columns exist, False otherwise
        """
        missing_columns = [col for col in deduplication_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Deduplication columns not found in dataframe: {missing_columns}")
        return True
    
    @staticmethod
    def validate_table_exists(table_name: str, available_tables: List[str]) -> bool:
        """
        Validate that a table exists in the configuration
        
        Args:
            table_name: Name of the table to check
            available_tables: List of available table names
            
        Returns:
            bool: True if table exists, False otherwise
        """
        if table_name not in available_tables:
            raise ValueError(f"Table not found: {table_name}")
        return True
    
    @staticmethod
    def validate_column_exists(df: DataFrame, column_name: str) -> bool:
        """
        Validate that a column exists in the dataframe
        
        Args:
            df: DataFrame to check
            column_name: Name of the column to validate
            
        Returns:
            bool: True if column exists, False otherwise
        """
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in dataframe. Available columns: {df.columns}")
        return True
    
    @staticmethod
    def validate_dataframe(df: DataFrame) -> bool:
        """
        Basic dataframe validation
        
        Args:
            df: DataFrame to validate
            
        Returns:
            bool: True if dataframe is valid, False otherwise
        """
        if df is None:
            raise ValueError("DataFrame cannot be None")
        
        if df.height == 0:
            raise ValueError("DataFrame cannot be empty")
        
        if df.width == 0:
            raise ValueError("DataFrame must have at least one column")
        
        return True
    
    @staticmethod
    def get_dataframe_info(df: DataFrame) -> Dict[str, Any]:
        """
        Get comprehensive information about a dataframe
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dict containing dataframe information
        """
        return {
            "shape": df.shape,
            "columns": df.columns,
            "dtypes": dict(zip(df.columns, [str(dtype) for dtype in df.dtypes])),
            "null_counts": df.null_count().to_dict(),
            "memory_usage_mb": round(df.estimated_size("mb"), 2) if hasattr(df, 'estimated_size') else "N/A"
        } 