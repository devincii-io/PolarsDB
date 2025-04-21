from faker import Faker
from datetime import datetime as dt, timedelta
from polars import DataFrame
import random
import os

fake = Faker()

def create_customer_data(num_records=100):
    """Create a dataset of customers with realistic data"""
    data = {
        "customer_id": [f"CUST-{i:05d}" for i in range(1, num_records + 1)],
        "first_name": [fake.first_name() for _ in range(num_records)],
        "last_name": [fake.last_name() for _ in range(num_records)],
        "email": [fake.email() for _ in range(num_records)],
        "phone": [fake.phone_number() for _ in range(num_records)],
        "address": [fake.address().replace('\n', ', ') for _ in range(num_records)],
        "signup_date": [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(num_records)],
        "last_purchase": [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_records)],
        "total_purchases": [random.randint(1, 50) for _ in range(num_records)],
        "total_spent": [round(random.uniform(50, 5000), 2) for _ in range(num_records)],
        "member_status": [random.choice(["Bronze", "Silver", "Gold", "Platinum"]) for _ in range(num_records)]
    }
    
    return DataFrame(data)

def create_update_data(original_df, update_percentage=0.3, new_records_percentage=0.2):
    """Create an update dataset that contains:
    1. A subset of the original data with some modified values (updates)
    2. Some completely new records (inserts)
    """
    num_original = original_df.height
    num_to_update = int(num_original * update_percentage)
    num_new = int(num_original * new_records_percentage)
    
    # Select random records to update - fix for Polars select method
    indices_to_update = random.sample(range(num_original), num_to_update)
    
    # Get rows at those indices - using a different approach
    # Create a temporary copy with index
    df_with_idx = original_df.with_row_count("_idx_")
    # Filter to keep only the rows at our randomly selected indices
    update_df = df_with_idx.filter(df_with_idx["_idx_"].is_in(indices_to_update))
    # Remove the temporary index column
    update_df = update_df.drop("_idx_").clone()
    
    # Modify some values in the update records
    update_data = update_df.to_dict(as_series=False)
    for i in range(len(update_data["customer_id"])):
        # Update some fields
        update_data["last_purchase"][i] = fake.date_time_between(
            start_date=update_data["last_purchase"][i], 
            end_date=dt.now()
        )
        update_data["total_purchases"][i] += random.randint(1, 10)
        update_data["total_spent"][i] += round(random.uniform(50, 500), 2)
        
        # Maybe upgrade membership status
        current_status = update_data["member_status"][i]
        status_levels = ["Bronze", "Silver", "Gold", "Platinum"]
        current_idx = status_levels.index(current_status)
        if current_idx < len(status_levels) - 1 and random.random() > 0.7:
            update_data["member_status"][i] = status_levels[current_idx + 1]
    
    # Create new records
    next_id = num_original + 1
    new_customer_ids = [f"CUST-{i:05d}" for i in range(next_id, next_id + num_new)]
    
    new_data = {
        "customer_id": new_customer_ids,
        "first_name": [fake.first_name() for _ in range(num_new)],
        "last_name": [fake.last_name() for _ in range(num_new)],
        "email": [fake.email() for _ in range(num_new)],
        "phone": [fake.phone_number() for _ in range(num_new)],
        "address": [fake.address().replace('\n', ', ') for _ in range(num_new)],
        "signup_date": [fake.date_time_between(start_date='-3m', end_date='now') for _ in range(num_new)],
        "last_purchase": [fake.date_time_between(start_date='-1m', end_date='now') for _ in range(num_new)],
        "total_purchases": [random.randint(1, 5) for _ in range(num_new)],
        "total_spent": [round(random.uniform(25, 300), 2) for _ in range(num_new)],
        "member_status": [random.choice(["Bronze", "Silver"]) for _ in range(num_new)]
    }
    
    # Combine updated and new records
    update_df = DataFrame(update_data)
    new_df = DataFrame(new_data)
    combined_df = update_df.vstack(new_df)
    
    return combined_df

if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # Create original customer dataset
    customers_df = create_customer_data(num_records=100)
    customers_df.write_csv("data/customers_original.csv")
    print(f"Created original customer dataset with {customers_df.height} records")
    
    # Create update dataset
    updates_df = create_update_data(customers_df)
    updates_df.write_csv("data/customers_updates.csv")
    print(f"Created update dataset with {updates_df.height} records")
    print(f"  - Updates: {int(0.3 * customers_df.height)} records")
    print(f"  - New records: {int(0.2 * customers_df.height)} records")


