#!/usr/bin/env python3
"""
E-commerce Event Generator for Kafka Simulated Pipeline
Generates realistic e-commerce events for orders, payments, inventory, and customers
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import argparse
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EcommerceEventGenerator:
    def __init__(self, kafka_servers: str = "kafka:29092", schema_registry_url: str = None):
        self.kafka_servers = kafka_servers
        self.schema_registry_url = schema_registry_url
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        
        # Sample data for realistic event generation
        self.customers = self._generate_customers(1000)
        self.products = self._generate_products(500)
        self.order_counter = 1
        self.payment_counter = 1
        
        logger.info(f"Event generator initialized with {len(self.customers)} customers and {len(self.products)} products")
    
    def _generate_customers(self, count: int) -> List[Dict]:
        """Generate sample customer data"""
        first_names = ["John", "Jane", "Mike", "Sarah", "David", "Emma", "Chris", "Lisa", "Alex", "Maria"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
        domains = ["gmail.com", "yahoo.com", "hotmail.com", "company.com", "example.org"]
        
        customers = []
        for i in range(1, count + 1):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"
            
            customer = {
                "customer_id": i,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                "address": f"{random.randint(100, 9999)} {random.choice(['Main St', 'Oak Ave', 'Park Rd', 'First St', 'Second Ave'])}",
                "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"]),
                "state": random.choice(["NY", "CA", "IL", "TX", "AZ", "PA"]),
                "zip_code": f"{random.randint(10000, 99999)}",
                "created_at": datetime.now().isoformat()
            }
            customers.append(customer)
        
        return customers
    
    def _generate_products(self, count: int) -> List[Dict]:
        """Generate sample product data"""
        categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys", "Beauty", "Automotive"]
        product_names = {
            "Electronics": ["Smartphone", "Laptop", "Tablet", "Headphones", "Speaker", "Camera", "TV", "Smartwatch"],
            "Clothing": ["T-Shirt", "Jeans", "Jacket", "Shoes", "Dress", "Sweater", "Hat", "Socks"],
            "Books": ["Novel", "Textbook", "Cookbook", "Biography", "Science Fiction", "Mystery", "Romance", "History"],
            "Home & Garden": ["Chair", "Table", "Lamp", "Plant", "Vase", "Pillow", "Curtains", "Rug"],
            "Sports": ["Basketball", "Soccer Ball", "Tennis Racket", "Yoga Mat", "Weights", "Running Shoes", "Bike", "Helmet"],
            "Toys": ["Action Figure", "Doll", "Puzzle", "Board Game", "LEGO Set", "Car Toy", "Ball", "Stuffed Animal"],
            "Beauty": ["Lipstick", "Foundation", "Shampoo", "Perfume", "Skincare", "Makeup Brush", "Nail Polish", "Moisturizer"],
            "Automotive": ["Car Parts", "Oil", "Tires", "Battery", "Air Filter", "Brake Pads", "Spark Plugs", "Car Cover"]
        }
        
        products = []
        for i in range(1, count + 1):
            category = random.choice(categories)
            name = random.choice(product_names[category])
            
            product = {
                "product_id": i,
                "name": f"{name} {random.choice(['Pro', 'Premium', 'Standard', 'Deluxe', 'Basic'])}",
                "description": f"High-quality {name.lower()} for everyday use",
                "category": category,
                "price": round(random.uniform(9.99, 999.99), 2),
                "stock_quantity": random.randint(0, 1000),
                "created_at": datetime.now().isoformat()
            }
            products.append(product)
        
        return products
    
    def generate_order_event(self) -> Dict[str, Any]:
        """Generate a realistic order event"""
        customer = random.choice(self.customers)
        product = random.choice(self.products)
        quantity = random.randint(1, 5)
        
        order_event = {
            "event_type": "OrderCreated",
            "order_id": self.order_counter,
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "price": product["price"],
            "total_amount": round(product["price"] * quantity, 2),
            "status": "PENDING",
            "timestamp": int(time.time() * 1000),
            "created_at": datetime.now().isoformat(),
            "metadata": {
                "customer_email": customer["email"],
                "product_name": product["name"],
                "product_category": product["category"],
                "order_source": random.choice(["web", "mobile", "api"])
            }
        }
        
        self.order_counter += 1
        return order_event
    
    def generate_payment_event(self, order_id: int = None) -> Dict[str, Any]:
        """Generate a payment event"""
        if order_id is None:
            order_id = random.randint(1, max(1, self.order_counter - 1))
        
        payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
        status_weights = [0.85, 0.1, 0.05]  # 85% success, 10% pending, 5% failed
        statuses = ["SUCCESS", "PENDING", "FAILED"]
        
        payment_event = {
            "event_type": "PaymentProcessed",
            "payment_id": self.payment_counter,
            "order_id": order_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "currency": "USD",
            "payment_method": random.choice(payment_methods),
            "status": random.choices(statuses, weights=status_weights)[0],
            "timestamp": int(time.time() * 1000),
            "processed_at": datetime.now().isoformat(),
            "metadata": {
                "gateway": "stripe",
                "transaction_id": str(uuid.uuid4()),
                "risk_score": round(random.uniform(0.0, 1.0), 3)
            }
        }
        
        self.payment_counter += 1
        return payment_event
    
    def generate_inventory_event(self) -> Dict[str, Any]:
        """Generate an inventory update event"""
        product = random.choice(self.products)
        event_types = ["StockUpdated", "LowStockAlert", "RestockRequested"]
        event_type = random.choice(event_types)
        
        if event_type == "LowStockAlert":
            new_quantity = random.randint(0, 10)  # Low stock
        else:
            new_quantity = random.randint(0, 1000)
        
        inventory_event = {
            "event_type": event_type,
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
            "previous_quantity": product["stock_quantity"],
            "new_quantity": new_quantity,
            "change_reason": random.choice(["sale", "restock", "adjustment", "return"]),
            "timestamp": int(time.time() * 1000),
            "updated_at": datetime.now().isoformat(),
            "metadata": {
                "warehouse_id": random.randint(1, 5),
                "updated_by": random.choice(["system", "admin", "api"])
            }
        }
        
        # Update product stock
        product["stock_quantity"] = new_quantity
        
        return inventory_event
    
    def generate_customer_event(self) -> Dict[str, Any]:
        """Generate a customer-related event"""
        customer = random.choice(self.customers)
        event_types = ["CustomerRegistered", "CustomerProfileUpdated", "CustomerPreferencesChanged"]
        event_type = random.choice(event_types)
        
        customer_event = {
            "event_type": event_type,
            "customer_id": customer["customer_id"],
            "first_name": customer["first_name"],
            "last_name": customer["last_name"],
            "email": customer["email"],
            "timestamp": int(time.time() * 1000),
            "updated_at": datetime.now().isoformat(),
            "metadata": {
                "source": random.choice(["web", "mobile", "api"]),
                "user_agent": random.choice(["Chrome", "Firefox", "Safari", "Edge", "Mobile"])
            }
        }
        
        return customer_event
    
    def generate_shipping_event(self, order_id: int = None) -> Dict[str, Any]:
        """Generate a shipping event"""
        if order_id is None:
            order_id = random.randint(1, max(1, self.order_counter - 1))
        
        event_types = ["ShipmentCreated", "ShipmentInTransit", "ShipmentDelivered", "DeliveryFailed"]
        event_type = random.choice(event_types)
        carriers = ["UPS", "FedEx", "USPS", "DHL", "Amazon"]
        
        shipping_event = {
            "event_type": event_type,
            "shipment_id": str(uuid.uuid4()),
            "order_id": order_id,
            "carrier": random.choice(carriers),
            "tracking_number": f"1Z{random.randint(100000000000, 999999999999)}",
            "status": event_type.replace("Shipment", "").replace("Delivery", "").upper(),
            "estimated_delivery": (datetime.now() + timedelta(days=random.randint(1, 7))).isoformat(),
            "timestamp": int(time.time() * 1000),
            "updated_at": datetime.now().isoformat(),
            "metadata": {
                "service_type": random.choice(["standard", "express", "overnight"]),
                "weight": round(random.uniform(0.1, 20.0), 2),
                "dimensions": {
                    "length": random.randint(5, 50),
                    "width": random.randint(5, 50),
                    "height": random.randint(5, 50)
                }
            }
        }
        
        return shipping_event
    
    def publish_event(self, topic: str, event: Dict[str, Any], key: str = None):
        """Publish event to Kafka topic"""
        try:
            future = self.producer.send(topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Published event to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
            
        except KafkaError as e:
            logger.error(f"Failed to publish event to {topic}: {e}")
            raise
    
    def run_simulation(self, duration_seconds: int = 3600, events_per_minute: int = 100):
        """Run the event simulation"""
        logger.info(f"Starting simulation for {duration_seconds} seconds at {events_per_minute} events/minute")
        
        start_time = time.time()
        event_count = 0
        
        # Event type distribution
        event_types = [
            ("orders", self.generate_order_event, 0.4),        # 40% orders
            ("payments", self.generate_payment_event, 0.25),   # 25% payments
            ("inventory", self.generate_inventory_event, 0.15), # 15% inventory
            ("customers", self.generate_customer_event, 0.1),   # 10% customers
            ("shipping", self.generate_shipping_event, 0.1)     # 10% shipping
        ]
        
        interval = 60.0 / events_per_minute  # Time between events
        
        try:
            while time.time() - start_time < duration_seconds:
                # Select event type based on weights
                weights = [weight for _, _, weight in event_types]
                selected_topic, event_generator, _ = random.choices(event_types, weights=weights)[0]
                
                # Generate and publish event
                event = event_generator()
                key = str(event.get("order_id") or event.get("customer_id") or event.get("product_id"))
                
                self.publish_event(selected_topic, event, key)
                event_count += 1
                
                if event_count % 100 == 0:
                    logger.info(f"Published {event_count} events")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        
        finally:
            self.producer.flush()
            self.producer.close()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Simulation completed: {event_count} events in {elapsed_time:.2f} seconds")
            logger.info(f"Average rate: {event_count / elapsed_time * 60:.2f} events/minute")

def main():
    parser = argparse.ArgumentParser(description="E-commerce Event Generator for Kafka")
    parser.add_argument("--kafka-servers", default="kafka:29092", help="Kafka bootstrap servers")
    parser.add_argument("--schema-registry", help="Schema Registry URL")
    parser.add_argument("--duration", type=int, default=3600, help="Simulation duration in seconds")
    parser.add_argument("--rate", type=int, default=100, help="Events per minute")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create and run event generator
    generator = EcommerceEventGenerator(
        kafka_servers=args.kafka_servers,
        schema_registry_url=args.schema_registry
    )
    
    generator.run_simulation(
        duration_seconds=args.duration,
        events_per_minute=args.rate
    )

if __name__ == "__main__":
    main()
