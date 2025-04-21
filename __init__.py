"""
PolarsDB - A lightweight CSV-based database system built on Polars

This package provides a simple database interface for CSV data using Polars dataframes.
"""

from src.client import DBClient
from src.config import DBConfig

__all__ = ['DBClient', 'DBConfig'] 