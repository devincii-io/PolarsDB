from polars import DataFrame, col
from typing import Dict, Any, List
from datetime import datetime as dt


class StatisticsCalculator:
    """Utility class for calculating operation statistics"""
    
    @staticmethod
    def calculate_insert_statistics(
        input_df: DataFrame,
        existing_df: DataFrame = None,
        deduplication_columns: List[str] = None,
        result_df: DataFrame = None
    ) -> Dict[str, Any]:
        """Calculate detailed statistics for insert operations"""
        
        old_size = existing_df.height if existing_df is not None else 0
        new_size = result_df.height if result_df is not None else input_df.height
        
        duplicates_found = 0
        rows_updated = 0
        rows_newly_inserted = input_df.height
        preserved_rows_count = old_size
        
        if existing_df is not None and deduplication_columns:
            # Find duplicates - rows that match deduplication columns
            duplicates = existing_df.join(
                input_df.select(deduplication_columns),
                on=deduplication_columns,
                how="inner"
            )
            duplicates_found = duplicates.height
            rows_updated = duplicates_found
            
            # Calculate preserved rows (existing rows that won't be updated)
            preserved_rows = existing_df.join(
                input_df.select(deduplication_columns),
                on=deduplication_columns,
                how="anti"
            )
            preserved_rows_count = preserved_rows.height
            
            # Count how many rows in new df are actually new vs updates
            new_keys_df = input_df.join(
                existing_df.select(deduplication_columns),
                on=deduplication_columns,
                how="anti"
            )
            rows_newly_inserted = new_keys_df.height
        
        return {
            "input_rows": input_df.height,
            "duplicates_found": duplicates_found,
            "rows_updated": rows_updated,
            "rows_newly_inserted": rows_newly_inserted,
            "rows_preserved": preserved_rows_count,
            "old_table_size": old_size,
            "new_table_size": new_size,
            "net_rows_added": new_size - old_size,
            "rejection_rate": 0,  # No rejections in insert operation
            "duplication_rate": round((duplicates_found / input_df.height) * 100, 2) if input_df.height > 0 else 0
        }
    
    @staticmethod
    def calculate_delete_statistics(
        original_df: DataFrame,
        filtered_df: DataFrame,
        deleted_count: int
    ) -> Dict[str, Any]:
        """Calculate detailed statistics for delete operations"""
        
        old_size = original_df.height
        new_size = filtered_df.height
        
        return {
            "old_table_size": old_size,
            "new_table_size": new_size,
            "rows_deleted": deleted_count,
            "rows_preserved": new_size,
            "deletion_rate": round((deleted_count / old_size) * 100, 2) if old_size > 0 else 0,
            "preservation_rate": round((new_size / old_size) * 100, 2) if old_size > 0 else 0
        }
    
    @staticmethod
    def calculate_query_statistics(
        result_df: DataFrame,
        execution_time: float,
        tables_used: List[str],
        namespace: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate detailed statistics for query operations"""
        
        tables_sizes = {}
        for table in tables_used:
            if table in namespace and hasattr(namespace[table], 'shape'):
                tables_sizes[table] = namespace[table].shape
        
        return {
            "result_shape": result_df.shape,
            "result_rows": result_df.height,
            "result_columns": result_df.width,
            "tables_accessed": len(tables_used),
            "tables_sizes": tables_sizes,
            "execution_time_seconds": round(execution_time, 4),
            "memory_usage_mb": round(result_df.estimated_size("mb"), 2) if hasattr(result_df, 'estimated_size') else "N/A"
        }
    
    @staticmethod
    def create_operation_summary(
        operation_type: str,
        statistics: Dict[str, Any],
        **kwargs
    ) -> str:
        """Create a human-readable summary for an operation"""
        
        if operation_type == "insert_data":
            input_rows = statistics.get("input_rows", 0)
            newly_inserted = statistics.get("rows_newly_inserted", 0)
            updated = statistics.get("rows_updated", 0)
            duplicates = statistics.get("duplicates_found", 0)
            return f"Inserted {input_rows} rows: {newly_inserted} new, {updated} updates, {duplicates} duplicates handled"
        
        elif operation_type == "delete_data_by_date":
            deleted = statistics.get("rows_deleted", 0)
            rate = statistics.get("deletion_rate", 0)
            min_date = kwargs.get("min_date")
            max_date = kwargs.get("max_date")
            return f"Deleted {deleted} rows ({rate}% of table) between {min_date} and {max_date}"
        
        elif operation_type == "delete_data_by_key":
            deleted = statistics.get("rows_deleted", 0)
            rate = statistics.get("deletion_rate", 0)
            key_column = kwargs.get("key_column")
            key_value = kwargs.get("key_value")
            return f"Deleted {deleted} rows ({rate}% of table) with {key_column}='{key_value}'"
        
        elif operation_type == "query":
            exec_time = statistics.get("execution_time_seconds", 0)
            result_rows = statistics.get("result_rows", 0)
            tables_count = statistics.get("tables_accessed", 0)
            return f"Query executed in {exec_time}s, returned {result_rows} rows from {tables_count} tables"
        
        else:
            return f"Operation {operation_type} completed" 