#!/usr/bin/env python3
"""
E-commerce Transaction Data Generator
Generates synthetic transaction data for HDFS pipeline testing
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()

# Configuration
RECORDS_PER_FILE = 10000
NUM_FILES = 5
OUTPUT_DIR = "/data/input"

# Product categories and price ranges
CATEGORIES = {
    "Electronics": (50, 2000),
    "Clothing": (10, 300),
    "Books": (5, 50),
    "Home & Garden": (15, 500),
    "Sports": (20, 800),
    "Beauty": (8, 150),
    "Toys": (5, 100),
    "Food": (2, 80)
}

LOCATIONS = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"
]

def generate_transaction():
    """Generate a single transaction record"""
    category = random.choice(list(CATEGORIES.keys()))
    price_range = CATEGORIES[category]
    price = round(random.uniform(price_range[0], price_range[1]), 2)
    quantity = random.randint(1, 5)
    
    # Generate timestamp within last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
    
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "product_id": f"PROD_{random.randint(10000, 99999)}",
        "category": category,
        "price": price,
        "quantity": quantity,
        "total_amount": round(price * quantity, 2),
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "location": random.choice(LOCATIONS)
    }

def main():
    """Generate sample data files"""
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Field names for CSV
    fieldnames = [
        "transaction_id", "customer_id", "product_id", "category",
        "price", "quantity", "total_amount", "timestamp", "location"
    ]
    
    print(f"Generating {NUM_FILES} files with {RECORDS_PER_FILE} records each...")
    
    for file_num in range(NUM_FILES):
        filename = f"{OUTPUT_DIR}/transactions_{file_num + 1:03d}.csv"
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for _ in range(RECORDS_PER_FILE):
                transaction = generate_transaction()
                writer.writerow(transaction)
        
        print(f"Generated {filename}")
    
    print(f"\nData generation complete!")
    print(f"Total records: {NUM_FILES * RECORDS_PER_FILE:,}")
    print(f"Files location: {OUTPUT_DIR}")
    
    # Generate summary statistics
    category_counts = {cat: 0 for cat in CATEGORIES.keys()}
    total_amount = 0
    
    # Quick stats from generated data
    for file_num in range(NUM_FILES):
        filename = f"{OUTPUT_DIR}/transactions_{file_num + 1:03d}.csv"
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                category_counts[row['category']] += 1
                total_amount += float(row['total_amount'])
    
    print(f"\nData Summary:")
    print(f"Total Revenue: ${total_amount:,.2f}")
    print(f"Category Distribution:")
    for category, count in category_counts.items():
        percentage = (count / (NUM_FILES * RECORDS_PER_FILE)) * 100
        print(f"  {category}: {count:,} ({percentage:.1f}%)")

if __name__ == "__main__":
    main()
