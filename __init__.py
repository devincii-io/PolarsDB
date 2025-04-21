"""
PolarsDB - A lightweight CSV-based database system built on Polars

This package provides a simple database interface for CSV data using Polars dataframes.
"""

# Import directly from src modules
from src.client import DBClient
from src.config import DBConfig

# Export these classes at the package level
__all__ = ['DBClient', 'DBConfig'] 