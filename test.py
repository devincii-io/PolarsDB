import os
import polars as pl
from src.config import DBConfig
from src.client import DBClient
from datetime import datetime as dt, timedelta

# Helper functions for testing
def print_separator(title):
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80)

def print_dataframe_summary(df, title):
    print(f"\n{title} - {df.height} rows, {len(df.columns)} columns")
    print(f"Sample data (first 5 rows):")
    print(df.head(5))

# Setup test environment
def setup_test_environment():
    print_separator("SETTING UP TEST ENVIRONMENT")
    
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Ensure config.json exists or will be created
    config_path = "config.json"
    
    # Define database configuration
    config = DBConfig(
        config_path=config_path,
        data_path="data",
        tables={
            "customers": {
                "deduplication_columns": ["customer_id"]
            }
        }
    )
    
    # Create DB client
    client = DBClient(config)
    
    return client

# Test data loading
def test_data_loading():
    print_separator("LOADING TEST DATA")
    
    # Load original data
    original_df = pl.read_csv("data/customers_original.csv", try_parse_dates=True)
    print_dataframe_summary(original_df, "Original customer data")
    
    # Load update data
    updates_df = pl.read_csv("data/customers_updates.csv", try_parse_dates=True)
    print_dataframe_summary(updates_df, "Update customer data")
    
    return original_df, updates_df

# Test insert operation
def test_insert_operation(client, df):
    print_separator("TESTING INSERT OPERATION")
    
    # Insert the original data
    print(f"Inserting {df.height} records into customers table...")
    client.insert_data("customers", df)
    
    # Verify insertion
    result_df = client.read_table("customers")
    print_dataframe_summary(result_df, "Data after insertion")
    
    # Verify operation history
    history = client.get_operation_history("customers")
    print(f"\nOperation history: {len(history)} operations recorded")
    for op in history:
        print(f"- {op['operation']} at {op['timestamp']}: {op['old_size']} → {op['new_size']} records")
    
    return result_df

# Test upsert operation
def test_upsert_operation(client, updates_df):
    print_separator("TESTING UPSERT OPERATION")
    
    # Get initial state
    before_df = client.read_table("customers")
    initial_count = before_df.height
    print(f"Before upsert: {initial_count} records")
    
    # Count number of updates vs new records
    update_ids = updates_df.select("customer_id").to_series().to_list()
    existing_ids = before_df.select("customer_id").to_series().to_list()
    updates_count = sum(1 for id in update_ids if id in existing_ids)
    new_count = len(update_ids) - updates_count
    
    print(f"Upserting {updates_df.height} records:")
    print(f"- {updates_count} updates to existing records")
    print(f"- {new_count} new records")
    
    # Perform upsert
    client.insert_data("customers", updates_df)
    
    # Verify results
    after_df = client.read_table("customers")
    print(f"\nAfter upsert: {after_df.height} records")
    print(f"Net change: {after_df.height - initial_count} records added")
    
    # Verify that updates were applied
    if updates_count > 0:
        print("\nVerifying a sample updated record:")
        sample_id = next(id for id in update_ids if id in existing_ids)
        before_record = before_df.filter(pl.col("customer_id") == sample_id)
        after_record = after_df.filter(pl.col("customer_id") == sample_id)
        
        # Compare before and after values
        print(f"Customer ID: {sample_id}")
        before_dict = before_record.to_dict(as_series=False)
        after_dict = after_record.to_dict(as_series=False)
        
        for key in before_dict:
            if key in after_dict and before_dict[key][0] != after_dict[key][0]:
                print(f"- {key}: {before_dict[key][0]} → {after_dict[key][0]}")
    
    # Verify operation history
    history = client.get_operation_history("customers")
    print(f"\nOperation history: {len(history)} operations recorded")
    for i, op in enumerate(history):
        print(f"{i+1}. {op['operation']} at {op['timestamp']}: {op['old_size']} → {op['new_size']} records")
    
    return after_df

# Test query operations
def test_query_operations(client):
    print_separator("TESTING QUERY OPERATIONS")
    
    # Simple filter
    print("\nQuery 1: Customers with total_purchases > 30")
    result1 = client.query("customers.filter(pl.col('total_purchases') > 30)")
    print(f"Found {result1.height} customers with > 30 purchases")
    print_dataframe_summary(result1, "High-volume customers")
    
    # Parameterized query
    print("\nQuery 2: Customers with membership status using parameter")
    result2 = client.query(
        "customers.filter(pl.col('member_status') == status)",
        {"status": "Gold"}
    )
    print(f"Found {result2.height} Gold customers")
    print_dataframe_summary(result2, "Gold customers")
    
    # Complex query with aggregation
    print("\nQuery 3: Average spending by membership status")
    result3 = client.query(
        "customers.group_by('member_status').agg(pl.col('total_spent').mean().alias('avg_spent')).sort('avg_spent', descending=True)"
    )
    print_dataframe_summary(result3, "Average spending by membership")
    
    # Verify operation history
    history = client.get_operation_history("customers")
    print(f"\nOperation history: {len(history)} operations recorded")
    for i, op in enumerate(history[-3:]):
        if op["operation"] == "query":
            query_info = op["args"]
            print(f"{i+1}. {op['operation']} at {op['timestamp']}: {query_info['query']}")
    
    return result1, result2, result3

# Test delete operations
def test_delete_operations(client):
    print_separator("TESTING DELETE OPERATIONS")
    
    # Get initial state
    before_df = client.read_table("customers")
    initial_count = before_df.height
    print(f"Before delete: {initial_count} records")
    
    # Test delete by date
    # Find a date range that should include some records
    mid_date = dt.now() - timedelta(days=180)
    test_start_date = mid_date - timedelta(days=30)
    test_end_date = mid_date + timedelta(days=30)
    
    print(f"\nDeleting records with last_purchase between {test_start_date} and {test_end_date}")
    client.delete_data_by_date("customers", "last_purchase", test_start_date, test_end_date)
    
    after_date_delete_df = client.read_table("customers")
    print(f"After date-based delete: {after_date_delete_df.height} records")
    print(f"Records deleted: {initial_count - after_date_delete_df.height}")
    
    # Test delete by key
    if after_date_delete_df.height > 0:
        # Choose a random customer to delete
        sample_id = after_date_delete_df.select("customer_id").to_series()[0]
        print(f"\nDeleting customer with ID: {sample_id}")
        client.delete_data_by_key("customers", "customer_id", sample_id)
        
        after_key_delete_df = client.read_table("customers") 
        print(f"After key-based delete: {after_key_delete_df.height} records")
        print(f"Records deleted: {after_date_delete_df.height - after_key_delete_df.height}")
    
    # Verify operation history
    history = client.get_operation_history("customers")
    print(f"\nOperation history: {len(history)} operations recorded")
    for i, op in enumerate(history[-2:]):
        print(f"{i+1}. {op['operation']} at {op['timestamp']}: {op['old_size']} → {op['new_size']} records")

def run_all_tests():
    # Setup
    client = setup_test_environment()
    
    # Load test data
    original_df, updates_df = test_data_loading()
    
    # Run tests
    test_insert_operation(client, original_df)
    test_upsert_operation(client, updates_df)
    test_query_operations(client)
    #test_delete_operations(client)
    
    print_separator("ALL TESTS COMPLETED")
    print("Final operation history:")
    
    all_history = client.get_all_operation_history()
    for table, ops in all_history.items():
        print(f"\nTable: {table} - {len(ops)} operations")
        for i, op in enumerate(ops):
            if op["operation"] in ["insert_data", "delete_data_by_date", "delete_data_by_key"]:
                print(f"{i+1}. {op['operation']} at {op['timestamp']}: {op['old_size']} → {op['new_size']} records")
            elif op["operation"] == "query":
                print(f"{i+1}. {op['operation']} at {op['timestamp']}: {op['args']['query'][:60]}...")

if __name__ == "__main__":
    run_all_tests()


