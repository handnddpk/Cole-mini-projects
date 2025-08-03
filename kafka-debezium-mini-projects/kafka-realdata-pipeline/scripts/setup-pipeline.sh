#!/bin/bash

# Kafka Real-Time Data Pipeline - Complete Setup Script
set -e

echo "🚀 Starting Kafka Real-Time Data Integration Pipeline Setup..."
echo "================================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check if required tools are installed
check_prerequisites() {
    print_header "Checking Prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    print_status "Prerequisites check passed ✓"
}

# Setup environment variables
setup_environment() {
    print_header "Setting up Environment Variables..."
    
    if [ ! -f .env ]; then
        print_warning ".env file not found. Creating from template..."
        cp .env.example .env
        print_warning "Please edit .env file with your API keys before continuing."
        echo "Press Enter to continue after editing .env file..."
        read
    fi
    
    # Source the environment file
    if [ -f .env ]; then
        source .env
        print_status "Environment variables loaded ✓"
    fi
}

# Create necessary directories
create_directories() {
    print_header "Creating Required Directories..."
    
    directories=(
        "data/mysql"
        "data/postgres"
        "data/mongodb"
        "data/clickhouse"
        "data/elasticsearch"
        "data/kafka-logs"
        "logs"
        "monitoring/grafana"
        "monitoring/prometheus"
    )
    
    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        print_status "Created directory: $dir"
    done
}

# Generate sample database initialization files
generate_sample_data() {
    print_header "Generating Sample Database Schemas..."
    
    # MySQL initialization
    cat > sql/mysql-init.sql << 'EOF'
-- E-commerce Database Schema for CDC
CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

-- Orders table
CREATE TABLE orders (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    status ENUM('PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED') DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_order_id (order_id),
    INDEX idx_customer_id (customer_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

-- Products table
CREATE TABLE products (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    description TEXT,
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_product_id (product_id),
    INDEX idx_category (category),
    INDEX idx_price (price)
);

-- Customers table
CREATE TABLE customers (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer_id (customer_id),
    INDEX idx_email (email),
    INDEX idx_city (city)
);

-- Insert sample data
INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, country) VALUES
('cust-001', 'John', 'Doe', 'john.doe@email.com', '+1-555-0101', 'New York', 'USA'),
('cust-002', 'Jane', 'Smith', 'jane.smith@email.com', '+1-555-0102', 'Los Angeles', 'USA'),
('cust-003', 'Mike', 'Johnson', 'mike.johnson@email.com', '+1-555-0103', 'Chicago', 'USA'),
('cust-004', 'Sarah', 'Williams', 'sarah.williams@email.com', '+1-555-0104', 'Houston', 'USA'),
('cust-005', 'David', 'Brown', 'david.brown@email.com', '+1-555-0105', 'Phoenix', 'USA');

INSERT INTO products (product_id, name, category, price, description, stock_quantity) VALUES
('prod-001', 'MacBook Pro 16"', 'Electronics', 2499.99, 'Apple MacBook Pro with M2 chip', 50),
('prod-002', 'iPhone 15 Pro', 'Electronics', 999.99, 'Latest iPhone with Pro features', 100),
('prod-003', 'AirPods Pro', 'Electronics', 249.99, 'Wireless earbuds with ANC', 200),
('prod-004', 'iPad Air', 'Electronics', 599.99, 'Powerful tablet for creativity', 75),
('prod-005', 'Apple Watch Series 9', 'Electronics', 399.99, 'Advanced smartwatch', 120);

INSERT INTO orders (order_id, customer_id, product_name, quantity, price, status) VALUES
('order-001', 'cust-001', 'MacBook Pro 16"', 1, 2499.99, 'PENDING'),
('order-002', 'cust-002', 'iPhone 15 Pro', 2, 999.99, 'CONFIRMED'),
('order-003', 'cust-003', 'AirPods Pro', 1, 249.99, 'SHIPPED'),
('order-004', 'cust-004', 'iPad Air', 1, 599.99, 'DELIVERED'),
('order-005', 'cust-005', 'Apple Watch Series 9', 1, 399.99, 'PENDING');
EOF

    # PostgreSQL initialization
    cat > sql/postgres-init.sql << 'EOF'
-- Inventory Database Schema for CDC
CREATE DATABASE inventory;
\c inventory;

-- Inventory table
CREATE TABLE inventory_items (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    reserved_quantity INTEGER NOT NULL DEFAULT 0,
    reorder_threshold INTEGER NOT NULL DEFAULT 10,
    reorder_quantity INTEGER NOT NULL DEFAULT 100,
    location VARCHAR(100) NOT NULL,
    supplier VARCHAR(255) NOT NULL,
    unit_cost DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_inventory_updated_at 
    BEFORE UPDATE ON inventory_items 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Sample inventory data
INSERT INTO inventory_items (product_id, product_name, category, quantity, reorder_threshold, reorder_quantity, location, supplier, unit_cost) VALUES
('prod-001', 'MacBook Pro 16"', 'Electronics', 45, 10, 20, 'Warehouse-A', 'Apple Inc.', 2000.00),
('prod-002', 'iPhone 15 Pro', 'Electronics', 98, 20, 50, 'Warehouse-A', 'Apple Inc.', 750.00),
('prod-003', 'AirPods Pro', 'Electronics', 199, 30, 100, 'Warehouse-B', 'Apple Inc.', 180.00),
('prod-004', 'iPad Air', 'Electronics', 74, 15, 30, 'Warehouse-A', 'Apple Inc.', 450.00),
('prod-005', 'Apple Watch Series 9', 'Electronics', 119, 25, 50, 'Warehouse-B', 'Apple Inc.', 300.00),
('prod-006', 'Samsung Galaxy S24', 'Electronics', 85, 20, 40, 'Warehouse-C', 'Samsung', 650.00),
('prod-007', 'Dell XPS 13', 'Electronics', 32, 10, 25, 'Warehouse-A', 'Dell', 900.00),
('prod-008', 'Sony WH-1000XM5', 'Electronics', 67, 15, 30, 'Warehouse-B', 'Sony', 250.00);

-- Create indexes for better performance
CREATE INDEX idx_inventory_product_id ON inventory_items(product_id);
CREATE INDEX idx_inventory_category ON inventory_items(category);
CREATE INDEX idx_inventory_quantity ON inventory_items(quantity);
CREATE INDEX idx_inventory_location ON inventory_items(location);
EOF

    # MongoDB initialization
    cat > sql/mongodb-init.js << 'EOF'
// MongoDB initialization for user profiles
use userprofiles;

// Create user profiles collection
db.createCollection("user_profiles");

// Insert sample user profiles
db.user_profiles.insertMany([
    {
        user_id: "user-001",
        customer_id: "cust-001",
        preferences: {
            categories: ["Electronics", "Books", "Sports"],
            brands: ["Apple", "Nike", "Sony"],
            price_range: { min: 50, max: 3000 },
            notifications: {
                email: true,
                sms: false,
                push: true
            }
        },
        demographics: {
            age_group: "25-34",
            location: { city: "New York", state: "NY", country: "USA" },
            income_bracket: "high"
        },
        behavior: {
            avg_order_value: 850.50,
            order_frequency: "monthly",
            last_login: new Date(),
            favorite_payment_method: "credit_card"
        },
        created_at: new Date(),
        updated_at: new Date()
    },
    {
        user_id: "user-002",
        customer_id: "cust-002",
        preferences: {
            categories: ["Fashion", "Beauty", "Electronics"],
            brands: ["Zara", "Sephora", "Samsung"],
            price_range: { min: 25, max: 1500 },
            notifications: {
                email: true,
                sms: true,
                push: true
            }
        },
        demographics: {
            age_group: "18-24",
            location: { city: "Los Angeles", state: "CA", country: "USA" },
            income_bracket: "medium"
        },
        behavior: {
            avg_order_value: 235.75,
            order_frequency: "weekly",
            last_login: new Date(),
            favorite_payment_method: "paypal"
        },
        created_at: new Date(),
        updated_at: new Date()
    }
]);

// Create indexes
db.user_profiles.createIndex({ "user_id": 1 }, { unique: true });
db.user_profiles.createIndex({ "customer_id": 1 });
db.user_profiles.createIndex({ "demographics.location.city": 1 });
db.user_profiles.createIndex({ "preferences.categories": 1 });

// Enable change streams (replica set required)
rs.initiate({
    _id: "rs0",
    members: [{ _id: 0, host: "mongodb-source:27017" }]
});
EOF

    print_status "Sample database schemas generated ✓"
}

# Start infrastructure services
start_infrastructure() {
    print_header "Starting Infrastructure Services..."
    
    print_status "Starting Kafka ecosystem..."
    docker-compose up -d zookeeper kafka schema-registry
    
    print_status "Waiting for Kafka to be ready..."
    sleep 30
    
    print_status "Starting databases..."
    docker-compose up -d mysql-source postgres-source mongodb-source
    
    print_status "Waiting for databases to initialize..."
    sleep 45
    
    print_status "Starting target systems..."
    docker-compose up -d elasticsearch clickhouse redis
    
    print_status "Waiting for target systems..."
    sleep 30
}

# Deploy Kafka Connect connectors
deploy_connectors() {
    print_header "Deploying Kafka Connect and Connectors..."
    
    # Start Kafka Connect
    docker-compose up -d connect
    
    print_status "Waiting for Kafka Connect to be ready..."
    sleep 60
    
    # Deploy CDC connectors
    print_status "Deploying Debezium MySQL connector..."
    curl -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @connectors/mysql-source-connector.json || print_warning "MySQL connector deployment may have failed"
    
    print_status "Deploying Debezium PostgreSQL connector..."
    curl -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @connectors/postgres-source-connector.json || print_warning "PostgreSQL connector deployment may have failed"
    
    print_status "Deploying MongoDB CDC connector..."
    curl -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @connectors/mongodb-source-connector.json || print_warning "MongoDB connector deployment may have failed"
    
    sleep 10
    
    # Deploy sink connectors
    print_status "Deploying Elasticsearch sink connector..."
    curl -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @connectors/elasticsearch-sink-connector.json || print_warning "Elasticsearch sink deployment may have failed"
    
    print_status "Deploying ClickHouse sink connector..."
    curl -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @connectors/clickhouse-sink-connector.json || print_warning "ClickHouse sink deployment may have failed"
}

# Start API collectors
start_api_collectors() {
    print_header "Starting API Data Collectors..."
    
    # Build and start API collector service
    if [ -d "api-collectors" ]; then
        print_status "Building API collector service..."
        docker-compose up -d api-collector
        
        print_status "API collectors started ✓"
    else
        print_warning "API collectors directory not found. Skipping..."
    fi
}

# Start monitoring services
start_monitoring() {
    print_header "Starting Monitoring Services..."
    
    print_status "Starting Prometheus and Grafana..."
    docker-compose up -d prometheus grafana
    
    print_status "Starting Confluent Control Center..."
    docker-compose up -d control-center
    
    sleep 20
    print_status "Monitoring services started ✓"
}

# Verify deployment
verify_deployment() {
    print_header "Verifying Deployment..."
    
    services=(
        "http://localhost:9021|Confluent Control Center"
        "http://localhost:8083|Kafka Connect"
        "http://localhost:8081|Schema Registry"
        "http://localhost:9200|Elasticsearch"
        "http://localhost:8123|ClickHouse"
        "http://localhost:3000|Grafana"
        "http://localhost:9090|Prometheus"
    )
    
    echo ""
    echo "🎯 Service Endpoints:"
    echo "===================="
    
    for service in "${services[@]}"; do
        url=$(echo $service | cut -d'|' -f1)
        name=$(echo $service | cut -d'|' -f2)
        
        if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "200\|302"; then
            print_status "✓ $name: $url"
        else
            print_warning "⚠ $name: $url (may still be starting)"
        fi
    done
    
    echo ""
    print_status "Checking Kafka Connect connectors..."
    sleep 5
    
    if curl -s http://localhost:8083/connectors | grep -q "\[\]"; then
        print_warning "No connectors deployed yet. Run './scripts/deploy-connectors.sh' manually if needed."
    else
        print_status "Connectors deployed: $(curl -s http://localhost:8083/connectors)"
    fi
}

# Display final information
show_completion_info() {
    print_header "Setup Complete! 🎉"
    
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                   🚀 KAFKA REAL-TIME PIPELINE                  ║${NC}"
    echo -e "${GREEN}║                        Setup Complete!                        ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    echo "📊 Access Points:"
    echo "• Confluent Control Center: http://localhost:9021"
    echo "• Kafka Connect REST API: http://localhost:8083"
    echo "• Schema Registry: http://localhost:8081"
    echo "• Elasticsearch: http://localhost:9200"
    echo "• ClickHouse: http://localhost:8123"
    echo "• Grafana Dashboard: http://localhost:3000 (admin/admin123)"
    echo "• Prometheus: http://localhost:9090"
    echo ""
    
    echo "🔗 Database Connections:"
    echo "• MySQL: localhost:3306 (root/debezium)"
    echo "• PostgreSQL: localhost:5432 (postgres/postgres)"
    echo "• MongoDB: localhost:27017 (admin/admin123)"
    echo "• Redis: localhost:6379"
    echo ""
    
    echo "🎯 Next Steps:"
    echo "1. Open Control Center to monitor topics and connectors"
    echo "2. Check connector status: curl http://localhost:8083/connectors"
    echo "3. View real-time data flow in Grafana dashboards"
    echo "4. Make changes to source databases to see CDC in action"
    echo ""
    
    echo "🛠️ Useful Commands:"
    echo "• View logs: docker-compose logs -f <service-name>"
    echo "• Restart service: docker-compose restart <service-name>"
    echo "• Stop all: docker-compose down"
    echo "• Clean up: docker-compose down -v"
    echo ""
    
    print_status "Happy streaming! 🌊"
}

# Main execution flow
main() {
    check_prerequisites
    setup_environment
    create_directories
    generate_sample_data
    start_infrastructure
    deploy_connectors
    start_api_collectors
    start_monitoring
    verify_deployment
    show_completion_info
}

# Execute main function
main "$@"
