import polars as pl
import re
from typing import Dict, Any, Optional, List, Set, TYPE_CHECKING
from datetime import datetime as dt
from os import path

if TYPE_CHECKING:
    from .config import DBConfig


class QueryExecutor:
    """Handles SQL query execution using Polars SQLContext with dynamic table loading"""
    
    def __init__(self, config: "DBConfig", table_reader_func):
        self.config = config
        self.read_table = table_reader_func
        self._table_functions = {
            'read_csv', 'read_parquet', 'read_json', 'read_ipc', 
            'read_ndjson', 'read_excel'
        }
    
    def _extract_table_names_from_sql(self, sql_query: str) -> Set[str]:
        """
        Extract table names referenced in the SQL query
        
        This function identifies:
        1. Tables in FROM clauses
        2. Tables in JOIN clauses  
        3. Tables in WITH/CTE clauses
        4. Tables in subqueries
        5. Tables used with table functions like read_csv()
        
        Returns:
            Set[str]: Set of table names found in the query
        """
        table_names = set()
        
        # Normalize the query - remove extra whitespace and convert to lowercase for parsing
        normalized_query = re.sub(r'\s+', ' ', sql_query.strip())
        
        # Pattern to match table names in FROM clauses
        # Matches: FROM table_name, FROM schema.table_name
        from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        from_matches = re.findall(from_pattern, normalized_query, re.IGNORECASE)
        for match in from_matches:
            # If it's schema.table, take just the table part
            table_name = match.split('.')[-1]
            table_names.add(table_name)
        
        # Pattern to match table names in JOIN clauses
        # Matches: JOIN table_name, LEFT JOIN table_name, etc.
        join_pattern = r'\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        join_matches = re.findall(join_pattern, normalized_query, re.IGNORECASE)
        for match in join_matches:
            table_name = match.split('.')[-1]
            table_names.add(table_name)
        
        # Pattern to match additional CTE table names after the first one
        cte_additional_pattern = r',\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS'
        cte_additional_matches = re.findall(cte_additional_pattern, normalized_query, re.IGNORECASE)
        for match in cte_additional_matches:
            table_names.add(match)
        
        # Check for table functions like read_csv('path')
        for func in self._table_functions:
            func_pattern = fr'\b{func}\s*\(\s*[\'"]([^\'"]+)[\'"]\s*\)'
            func_matches = re.findall(func_pattern, normalized_query, re.IGNORECASE)
            for file_path in func_matches:
                # Extract table name from file path (remove path and extension)
                table_name = path.splitext(path.basename(file_path))[0]
                table_names.add(table_name)
        
        # Filter to only include tables that exist in our configuration
        valid_tables = set(self.config.tables.keys())
        return table_names.intersection(valid_tables)
    
    def execute_query(self, sql_query: str, params: Optional[Dict[str, Any]] = None) -> tuple[pl.DataFrame, float, List[str]]:
        """
        Execute a SQL query using Polars SQLContext, loading only referenced tables
        
        Parameters:
        -----------
        sql_query : str
            SQL query string to execute
        params : Dict[str, Any], optional
            Parameters for the query (not directly used in SQL but available in context)
            
        Returns:
        --------
        tuple: (result_dataframe, execution_time_seconds, tables_used)
        """
        # Extract table names from the SQL query
        referenced_tables = self._extract_table_names_from_sql(sql_query)
        
        # Create SQLContext with only the referenced tables
        tables_dict = {}
        tables_loaded = []
        
        for table_name in referenced_tables:
            try:
                # Load table as LazyFrame for better performance
                table_path = path.join(self.config.data_path, f"{table_name}.csv")
                if path.exists(table_path):
                    tables_dict[table_name] = pl.scan_csv(
                        table_path,
                        try_parse_dates=True,
                        infer_schema_length=5000,
                        infer_schema=True
                    )
                    tables_loaded.append(table_name)
                else:
                    # Try to load with the table reader function
                    df = self.read_table(table_name)
                    tables_dict[table_name] = df.lazy()
                    tables_loaded.append(table_name)
            except Exception as e:
                print(f"Warning: Could not load table '{table_name}': {str(e)}")
                continue
        
        if not tables_dict:
            raise ValueError("No valid tables found in the SQL query or no tables could be loaded")
        
        # Execute the query with timing
        start_time = dt.now()
        
        # Create SQLContext with only the tables we need
        with pl.SQLContext(frames=tables_dict, eager=True) as ctx:
            result = ctx.execute(sql_query)
        
        execution_time = (dt.now() - start_time).total_seconds()
        
        return result, execution_time, tables_loaded
    
    def validate_query(self, sql_query: str) -> bool:
        """
        Validate SQL query for security and basic syntax
        
        Returns:
            bool: True if query appears valid and safe
        """
        # Check for dangerous patterns
        dangerous_patterns = [
            r'\bDROP\s+(?:TABLE|DATABASE|SCHEMA)\b',  # DROP statements
            r'\bDELETE\s+FROM\b',  # DELETE statements
            r'\bUPDATE\s+\w+\s+SET\b',  # UPDATE statements
            r'\bINSERT\s+INTO\b',  # INSERT statements
            r'\bTRUNCATE\s+TABLE\b',  # TRUNCATE statements
            r'\bALTER\s+TABLE\b',  # ALTER statements
            r'\bCREATE\s+(?:TABLE|VIEW|INDEX)\b',  # CREATE statements (except CREATE TABLE AS SELECT which is allowed)
            r'\bEXEC(?:UTE)?\b',  # EXECUTE statements
            r'[;\'"]\s*(?:DROP|DELETE|UPDATE|INSERT|TRUNCATE|ALTER|CREATE)',  # SQL injection patterns
        ]
        
        query_upper = sql_query.upper()
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE):
                # Allow CREATE TABLE AS SELECT
                if 'CREATE TABLE' in query_upper and 'AS SELECT' in query_upper:
                    continue
                return False
        
        # Basic syntax checks
        if sql_query.strip() == "":
            return False
            
        # Check for balanced parentheses
        if sql_query.count('(') != sql_query.count(')'):
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
    
    def show_available_tables(self) -> pl.DataFrame:
        """Show all available tables in the database"""
        tables_data = []
        for table_name, table_config in self.config.tables.items():
            table_path = path.join(self.config.data_path, f"{table_name}.csv")
            exists = path.exists(table_path)
            
            if exists:
                try:
                    # Get basic info about the table
                    df = pl.scan_csv(table_path, infer_schema_length=1)
                    shape = df.select(pl.len()).collect().item()
                    columns = df.columns
                    tables_data.append({
                        "table_name": table_name,
                        "exists": True,
                        "row_count": shape,
                        "column_count": len(columns),
                        "columns": ", ".join(columns[:5]) + ("..." if len(columns) > 5 else "")
                    })
                except Exception:
                    tables_data.append({
                        "table_name": table_name,
                        "exists": False,
                        "row_count": None,
                        "column_count": None,
                        "columns": "Error reading table"
                    })
            else:
                tables_data.append({
                    "table_name": table_name,
                    "exists": False,
                    "row_count": None,
                    "column_count": None,
                    "columns": "File not found"
                })
        
        return pl.DataFrame(tables_data)
    
    def explain_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Explain what tables would be loaded for a given SQL query without executing it
        
        Returns:
            Dict containing information about the query plan
        """
        referenced_tables = self._extract_table_names_from_sql(sql_query)
        available_tables = set(self.config.tables.keys())
        
        valid_tables = referenced_tables.intersection(available_tables)
        invalid_tables = referenced_tables - available_tables
        
        return {
            "sql_query": sql_query,
            "tables_referenced": list(referenced_tables),
            "tables_available": list(available_tables),
            "tables_to_load": list(valid_tables),
            "invalid_tables": list(invalid_tables),
            "is_valid": len(invalid_tables) == 0 and len(valid_tables) > 0,
            "estimated_tables_loaded": len(valid_tables)
        } 