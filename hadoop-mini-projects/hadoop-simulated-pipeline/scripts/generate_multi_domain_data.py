#!/usr/bin/env python3
"""
Multi-Domain Data Generator for Hadoop Ecosystem
Generates realistic business data across multiple domains
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import mysql.connector
from mysql.connector import Error
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

# Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql-source')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'hadoop')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'retail_db')

# Data generation parameters
NUM_CUSTOMERS = 50000
NUM_PRODUCTS = 10000
NUM_SUPPLIERS = 500
NUM_TRANSACTIONS = 200000
NUM_WEB_SESSIONS = 100000

# Product categories and their characteristics
PRODUCT_CATEGORIES = {
    "Electronics": {
        "subcategories": ["Smartphones", "Laptops", "Tablets", "Headphones", "Cameras"],
        "price_range": (50, 2000),
        "cost_margin": 0.7
    },
    "Clothing": {
        "subcategories": ["Men's Apparel", "Women's Apparel", "Shoes", "Accessories"],
        "price_range": (10, 300),
        "cost_margin": 0.5
    },
    "Books": {
        "subcategories": ["Fiction", "Non-Fiction", "Educational", "Children's Books"],
        "price_range": (5, 50),
        "cost_margin": 0.6
    },
    "Home & Garden": {
        "subcategories": ["Furniture", "Kitchen", "Garden Tools", "Decor"],
        "price_range": (15, 1000),
        "cost_margin": 0.6
    },
    "Sports": {
        "subcategories": ["Fitness", "Outdoor", "Team Sports", "Individual Sports"],
        "price_range": (20, 800),
        "cost_margin": 0.6
    },
    "Beauty": {
        "subcategories": ["Skincare", "Makeup", "Hair Care", "Fragrances"],
        "price_range": (8, 150),
        "cost_margin": 0.4
    }
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Cash"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
WEB_ACTIONS = ["view", "add_to_cart", "remove_from_cart", "purchase", "search", "filter", "logout"]

def create_database_connection():
    """Create MySQL database connection"""
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        if connection.is_connected():
            logger.info("Successfully connected to MySQL database")
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None

def create_tables(connection):
    """Create database tables"""
    cursor = connection.cursor()
    
    # Suppliers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_id INT PRIMARY KEY,
            name VARCHAR(255),
            contact_email VARCHAR(255),
            contact_phone VARCHAR(20),
            city VARCHAR(100),
            country VARCHAR(100),
            rating DECIMAL(3,2),
            contract_date DATE
        )
    """)
    
    # Products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(100),
            subcategory VARCHAR(100),
            price DECIMAL(10,2),
            cost DECIMAL(10,2),
            supplier_id INT,
            stock_quantity INT,
            created_date DATE,
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        )
    """)
    
    # Customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            phone VARCHAR(20),
            age INT,
            gender VARCHAR(10),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            registration_date DATE,
            loyalty_tier VARCHAR(20)
        )
    """)
    
    # Transactions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(36) PRIMARY KEY,
            customer_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            payment_method VARCHAR(50),
            transaction_date DATETIME,
            city VARCHAR(100),
            state VARCHAR(100),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)
    
    # Web sessions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS web_sessions (
            session_id VARCHAR(36) PRIMARY KEY,
            customer_id INT,
            page VARCHAR(255),
            action VARCHAR(50),
            session_timestamp DATETIME,
            ip_address VARCHAR(15),
            user_agent TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    connection.commit()
    logger.info("Database tables created successfully")

def generate_suppliers(connection, num_suppliers):
    """Generate supplier data"""
    logger.info(f"Generating {num_suppliers} suppliers...")
    cursor = connection.cursor()
    
    suppliers = []
    for i in range(1, num_suppliers + 1):
        supplier = (
            i,
            fake.company(),
            fake.company_email(),
            fake.phone_number(),
            fake.city(),
            fake.country(),
            round(random.uniform(3.0, 5.0), 2),
            fake.date_between(start_date='-5y', end_date='today')
        )
        suppliers.append(supplier)
    
    cursor.executemany("""
        INSERT INTO suppliers 
        (supplier_id, name, contact_email, contact_phone, city, country, rating, contract_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, suppliers)
    
    connection.commit()
    logger.info(f"Inserted {num_suppliers} suppliers")

def generate_products(connection, num_products):
    """Generate product data"""
    logger.info(f"Generating {num_products} products...")
    cursor = connection.cursor()
    
    products = []
    for i in range(1, num_products + 1):
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        category_info = PRODUCT_CATEGORIES[category]
        subcategory = random.choice(category_info["subcategories"])
        
        price = round(random.uniform(*category_info["price_range"]), 2)
        cost = round(price * category_info["cost_margin"], 2)
        supplier_id = random.randint(1, NUM_SUPPLIERS)
        
        product = (
            i,
            fake.catch_phrase(),
            category,
            subcategory,
            price,
            cost,
            supplier_id,
            random.randint(0, 1000),
            fake.date_between(start_date='-2y', end_date='today')
        )
        products.append(product)
    
    cursor.executemany("""
        INSERT INTO products 
        (product_id, name, category, subcategory, price, cost, supplier_id, stock_quantity, created_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, products)
    
    connection.commit()
    logger.info(f"Inserted {num_products} products")

def generate_customers(connection, num_customers):
    """Generate customer data"""
    logger.info(f"Generating {num_customers} customers...")
    cursor = connection.cursor()
    
    customers = []
    for i in range(1, num_customers + 1):
        gender = random.choice(['Male', 'Female', 'Other'])
        first_name = fake.first_name_male() if gender == 'Male' else fake.first_name_female() if gender == 'Female' else fake.first_name()
        
        customer = (
            i,
            first_name,
            fake.last_name(),
            fake.email(),
            fake.phone_number(),
            random.randint(18, 80),
            gender,
            fake.city(),
            fake.state(),
            fake.country(),
            fake.date_between(start_date='-3y', end_date='today'),
            random.choice(LOYALTY_TIERS)
        )
        customers.append(customer)
    
    cursor.executemany("""
        INSERT INTO customers 
        (customer_id, first_name, last_name, email, phone, age, gender, city, state, country, registration_date, loyalty_tier)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, customers)
    
    connection.commit()
    logger.info(f"Inserted {num_customers} customers")

def generate_transactions(connection, num_transactions):
    """Generate transaction data"""
    logger.info(f"Generating {num_transactions} transactions...")
    cursor = connection.cursor()
    
    transactions = []
    for _ in range(num_transactions):
        customer_id = random.randint(1, NUM_CUSTOMERS)
        product_id = random.randint(1, NUM_PRODUCTS)
        quantity = random.randint(1, 5)
        
        # Get product price
        cursor.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
        result = cursor.fetchone()
        unit_price = result[0] if result else 100.0
        
        total_amount = round(unit_price * quantity, 2)
        
        transaction = (
            str(uuid.uuid4()),
            customer_id,
            product_id,
            quantity,
            unit_price,
            total_amount,
            random.choice(PAYMENT_METHODS),
            fake.date_time_between(start_date='-1y', end_date='now'),
            fake.city(),
            fake.state()
        )
        transactions.append(transaction)
    
    cursor.executemany("""
        INSERT INTO transactions 
        (transaction_id, customer_id, product_id, quantity, unit_price, total_amount, payment_method, transaction_date, city, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, transactions)
    
    connection.commit()
    logger.info(f"Inserted {num_transactions} transactions")

def generate_web_sessions(connection, num_sessions):
    """Generate web session data"""
    logger.info(f"Generating {num_sessions} web sessions...")
    cursor = connection.cursor()
    
    web_sessions = []
    pages = ['/home', '/products', '/cart', '/checkout', '/account', '/search', '/category']
    
    for _ in range(num_sessions):
        session = (
            str(uuid.uuid4()),
            random.randint(1, NUM_CUSTOMERS),
            random.choice(pages),
            random.choice(WEB_ACTIONS),
            fake.date_time_between(start_date='-6m', end_date='now'),
            fake.ipv4(),
            fake.user_agent()
        )
        web_sessions.append(session)
    
    cursor.executemany("""
        INSERT INTO web_sessions 
        (session_id, customer_id, page, action, session_timestamp, ip_address, user_agent)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, web_sessions)
    
    connection.commit()
    logger.info(f"Inserted {num_sessions} web sessions")

def export_to_csv(connection):
    """Export data to CSV files for additional processing"""
    logger.info("Exporting data to CSV files...")
    
    cursor = connection.cursor()
    
    # Export tables to CSV
    tables = ['suppliers', 'products', 'customers', 'transactions', 'web_sessions']
    
    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"DESCRIBE {table}")
        columns = [col[0] for col in cursor.fetchall()]
        
        # Write to CSV
        filename = f'/data/exports/{table}.csv'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)
        
        logger.info(f"Exported {len(rows)} rows to {filename}")

def generate_summary_report(connection):
    """Generate a summary report of the generated data"""
    cursor = connection.cursor()
    
    report = {
        "generation_timestamp": datetime.now().isoformat(),
        "data_summary": {}
    }
    
    tables = ['suppliers', 'products', 'customers', 'transactions', 'web_sessions']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        report["data_summary"][table] = count
    
    # Additional analytics
    cursor.execute("""
        SELECT category, COUNT(*) as product_count, 
               AVG(price) as avg_price, SUM(stock_quantity) as total_stock
        FROM products 
        GROUP BY category
    """)
    
    category_stats = {}
    for row in cursor.fetchall():
        category_stats[row[0]] = {
            "product_count": row[1],
            "avg_price": float(row[2]),
            "total_stock": row[3]
        }
    
    report["category_analysis"] = category_stats
    
    # Revenue analysis
    cursor.execute("""
        SELECT DATE(transaction_date) as date, 
               COUNT(*) as transaction_count,
               SUM(total_amount) as daily_revenue
        FROM transactions 
        GROUP BY DATE(transaction_date)
        ORDER BY date DESC
        LIMIT 30
    """)
    
    daily_revenue = []
    for row in cursor.fetchall():
        daily_revenue.append({
            "date": row[0].isoformat(),
            "transaction_count": row[1],
            "daily_revenue": float(row[2])
        })
    
    report["recent_daily_revenue"] = daily_revenue
    
    # Save report
    with open('/data/exports/generation_report.json', 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info("Generated comprehensive data summary report")

def main():
    """Main data generation function"""
    logger.info("Starting multi-domain data generation...")
    
    # Connect to database
    connection = create_database_connection()
    if not connection:
        logger.error("Failed to connect to database")
        return
    
    try:
        # Create tables
        create_tables(connection)
        
        # Generate data in dependency order
        generate_suppliers(connection, NUM_SUPPLIERS)
        generate_products(connection, NUM_PRODUCTS)
        generate_customers(connection, NUM_CUSTOMERS)
        generate_transactions(connection, NUM_TRANSACTIONS)
        generate_web_sessions(connection, NUM_WEB_SESSIONS)
        
        # Export to CSV
        export_to_csv(connection)
        
        # Generate report
        generate_summary_report(connection)
        
        logger.info("Multi-domain data generation completed successfully!")
        
        # Print summary
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM suppliers")
        suppliers_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM products")
        products_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM customers")
        customers_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM transactions")
        transactions_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM web_sessions")
        sessions_count = cursor.fetchone()[0]
        
        print("\n" + "="*50)
        print("DATA GENERATION SUMMARY")
        print("="*50)
        print(f"Suppliers: {suppliers_count:,}")
        print(f"Products: {products_count:,}")
        print(f"Customers: {customers_count:,}")
        print(f"Transactions: {transactions_count:,}")
        print(f"Web Sessions: {sessions_count:,}")
        print(f"Total Records: {suppliers_count + products_count + customers_count + transactions_count + sessions_count:,}")
        print("="*50)
        
    except Exception as e:
        logger.error(f"Error during data generation: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()
