from typing import Dict, List, Any, Optional
from datetime import datetime as dt


class OperationHistoryManager:
    """Manages operation history tracking and analysis for database operations"""
    
    def __init__(self):
        self.operation_history: Dict[str, List[Dict[str, Any]]] = {}
    
    def record_operation(self, table_name: str, operation_record: Dict[str, Any]):
        """Record an operation in the history"""
        if table_name not in self.operation_history:
            self.operation_history[table_name] = []
        self.operation_history[table_name].append(operation_record)
    
    def record_operation_multiple_tables(self, table_names: List[str], operation_record: Dict[str, Any]):
        """Record an operation that affects multiple tables"""
        for table_name in table_names:
            self.record_operation(table_name, operation_record)
    
    def get_history(self, table_name: str) -> List[Dict[str, Any]]:
        """Get operation history for a specific table"""
        return self.operation_history.get(table_name, [])
    
    def get_all_history(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get complete operation history"""
        return self.operation_history
    
    def get_operation_summary(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get a summary of operations performed on a table or all tables"""
        if table_name:
            if table_name not in self.operation_history:
                return {
                    "table": table_name, 
                    "total_operations": 0, 
                    "operations": {},
                    "total_duplicates_handled": 0,
                    "total_rows_inserted": 0,
                    "total_rows_deleted": 0,
                    "total_queries": 0
                }
            
            operations = self.operation_history[table_name]
        else:
            operations = []
            for table_ops in self.operation_history.values():
                operations.extend(table_ops)
        
        summary = {
            "table": table_name or "ALL",
            "total_operations": len(operations),
            "operations": {},
            "total_duplicates_handled": 0,
            "total_rows_inserted": 0,
            "total_rows_deleted": 0,
            "total_queries": 0
        }
        
        for op in operations:
            op_type = op["operation"]
            if op_type not in summary["operations"]:
                summary["operations"][op_type] = 0
            summary["operations"][op_type] += 1
            
            # Aggregate statistics
            if "statistics" in op:
                stats = op["statistics"]
                if op_type == "insert_data":
                    summary["total_duplicates_handled"] += stats.get("duplicates_found", 0)
                    summary["total_rows_inserted"] += stats.get("rows_newly_inserted", 0)
                elif op_type in ["delete_data_by_date", "delete_data_by_key"]:
                    summary["total_rows_deleted"] += stats.get("rows_deleted", 0)
                elif op_type == "query":
                    summary["total_queries"] += 1
        
        return summary
    
    def get_recent_operations(self, table_name: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most recent operations for a table or all tables"""
        if table_name:
            if table_name not in self.operation_history:
                return []
            operations = self.operation_history[table_name]
        else:
            operations = []
            for table_ops in self.operation_history.values():
                operations.extend(table_ops)
        
        # Sort by timestamp (most recent first)
        sorted_ops = sorted(operations, key=lambda x: x["timestamp"], reverse=True)
        return sorted_ops[:limit]
    
    def print_operation_history(self, table_name: Optional[str] = None, detailed: bool = False):
        """Print a formatted view of operation history"""
        if table_name:
            operations = self.get_history(table_name)
            print(f"\n=== Operation History for Table: {table_name} ===")
        else:
            print("\n=== Complete Operation History ===")
            operations = []
            for table, table_ops in self.operation_history.items():
                for op in table_ops:
                    op_copy = op.copy()
                    op_copy["table"] = table
                    operations.append(op_copy)
            operations.sort(key=lambda x: x["timestamp"])
        
        if not operations:
            print("No operations recorded.")
            return
        
        for i, op in enumerate(operations, 1):
            table_info = f" ({op['table']})" if 'table' in op else ""
            print(f"\n{i}. {op['operation'].upper()}{table_info} - {op['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            if "summary" in op:
                print(f"   Summary: {op['summary']}")
            
            if detailed and "statistics" in op:
                print("   Statistics:")
                for key, value in op["statistics"].items():
                    print(f"     {key}: {value}")
        
        # Print summary
        summary = self.get_operation_summary(table_name)
        print(f"\n=== Summary ===")
        print(f"Total operations: {summary['total_operations']}")
        print(f"Operations breakdown: {summary['operations']}")
        if summary['total_duplicates_handled'] > 0:
            print(f"Total duplicates handled: {summary['total_duplicates_handled']}")
        if summary['total_rows_inserted'] > 0:
            print(f"Total rows inserted: {summary['total_rows_inserted']}")
        if summary['total_rows_deleted'] > 0:
            print(f"Total rows deleted: {summary['total_rows_deleted']}")
        if summary['total_queries'] > 0:
            print(f"Total queries executed: {summary['total_queries']}") 