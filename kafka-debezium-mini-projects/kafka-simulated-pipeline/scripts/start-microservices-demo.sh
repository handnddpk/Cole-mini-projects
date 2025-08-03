#!/bin/bash

# Start microservices demo for Kafka Event-Driven Pipeline
# This script demonstrates the complete event-driven architecture

set -e

echo "🚀 Starting Kafka Event-Driven Microservices Demo"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to create Kafka topics
create_kafka_topics() {
    echo -e "${BLUE}Creating Kafka topics...${NC}"
    
    topics=("orders" "payments" "inventory" "customers" "shipping")
    
    for topic in "${topics[@]}"; do
        echo "Creating topic: $topic"
        docker exec kafka-sim-broker kafka-topics --create \
            --bootstrap-server localhost:9092 \
            --topic $topic \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists
    done
    
    echo -e "${GREEN}✓ All topics created${NC}"
}

# Function to register Avro schemas
register_schemas() {
    echo -e "${BLUE}Registering Avro schemas...${NC}"
    
    # Order Event Schema
    ORDER_SCHEMA='{
        "type": "record",
        "name": "OrderEvent",
        "namespace": "com.ecommerce.events",
        "fields": [
            {"name": "event_type", "type": "string"},
            {"name": "order_id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "product_id", "type": "long"},
            {"name": "quantity", "type": "int"},
            {"name": "price", "type": "double"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
        ]
    }'
    
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "{\"schema\":\"$(echo $ORDER_SCHEMA | sed 's/"/\\"/g')\"}" \
        http://localhost:8081/subjects/orders-value/versions
    
    echo -e "${GREEN}✓ Schemas registered${NC}"
}

# Function to start event generation
start_event_generation() {
    echo -e "${BLUE}Starting event generation...${NC}"
    
    # Start the event generator container
    docker-compose up -d event-generator
    
    echo -e "${GREEN}✓ Event generator started${NC}"
}

# Function to demonstrate order processing flow
demonstrate_order_flow() {
    echo -e "${BLUE}Demonstrating order processing flow...${NC}"
    
    # Create a sample order via API
    echo "Creating sample order..."
    ORDER_RESPONSE=$(curl -s -X POST http://localhost:8090/api/orders \
        -H "Content-Type: application/json" \
        -d '{
            "customerId": 1,
            "productId": 1,
            "quantity": 2,
            "price": 29.99
        }')
    
    echo "Order created: $ORDER_RESPONSE"
    
    # Extract order ID
    ORDER_ID=$(echo $ORDER_RESPONSE | jq -r '.id')
    
    if [ "$ORDER_ID" != "null" ]; then
        # Update order status to simulate processing
        echo "Updating order status to CONFIRMED..."
        curl -s -X PUT http://localhost:8090/api/orders/$ORDER_ID/status \
            -H "Content-Type: application/json" \
            -d '{"status": "CONFIRMED"}'
        
        echo -e "${GREEN}✓ Order flow demonstrated${NC}"
    else
        echo -e "${YELLOW}⚠ Could not create order (service may not be ready)${NC}"
    fi
}

# Function to show Kafka topics and messages
show_kafka_activity() {
    echo -e "${BLUE}Showing recent Kafka activity...${NC}"
    
    topics=("orders" "payments" "inventory")
    
    for topic in "${topics[@]}"; do
        echo -e "${YELLOW}Recent messages in topic: $topic${NC}"
        docker exec kafka-sim-broker kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic $topic \
            --from-beginning \
            --max-messages 5 \
            --timeout-ms 5000 2>/dev/null || echo "No messages yet in $topic"
        echo ""
    done
}

# Function to display service status
show_service_status() {
    echo -e "${BLUE}Service Status:${NC}"
    
    services=("kafka-sim-order-service:8090" "kafka-sim-inventory-service:8091" "kafka-sim-payment-service:8092")
    
    for service in "${services[@]}"; do
        service_name=$(echo $service | cut -d':' -f1)
        port=$(echo $service | cut -d':' -f2)
        
        if curl -s -f http://localhost:$port/actuator/health > /dev/null; then
            echo -e "${GREEN}✓ $service_name is healthy${NC}"
        else
            echo -e "${RED}✗ $service_name is not responding${NC}"
        fi
    done
}

# Function to show monitoring URLs
show_monitoring_urls() {
    echo -e "${BLUE}📊 Monitoring URLs:${NC}"
    echo "🔗 Kafka UI:              http://localhost:8080"
    echo "🔗 Schema Registry:       http://localhost:8081"
    echo "🔗 Grafana Dashboard:     http://localhost:3000 (admin/admin123)"
    echo "🔗 Prometheus Metrics:    http://localhost:9090"
    echo ""
    echo -e "${BLUE}🏗️ Service APIs:${NC}"
    echo "🔗 Order Service:         http://localhost:8090/api/orders"
    echo "🔗 Inventory Service:     http://localhost:8091/api/inventory"
    echo "🔗 Payment Service:       http://localhost:8092/api/payments"
    echo "🔗 Shipping Service:      http://localhost:8093/api/shipping"
}

# Main execution
main() {
    echo -e "${GREEN}Starting Kafka Event-Driven Microservices Demo...${NC}"
    
    # Wait for services to be ready
    echo "Waiting for services to be ready..."
    sleep 30
    
    # Create topics
    create_kafka_topics
    
    # Register schemas (optional, can work without for JSON)
    # register_schemas
    
    # Start event generation
    start_event_generation
    
    # Wait a bit for events to start flowing
    sleep 10
    
    # Demonstrate order flow
    demonstrate_order_flow
    
    # Show Kafka activity
    show_kafka_activity
    
    # Show service status
    show_service_status
    
    # Show monitoring URLs
    show_monitoring_urls
    
    echo -e "${GREEN}🎉 Demo setup complete!${NC}"
    echo -e "${YELLOW}💡 Tips:${NC}"
    echo "  - Monitor events in Kafka UI: http://localhost:8080"
    echo "  - View dashboards in Grafana: http://localhost:3000"
    echo "  - Create orders via API: curl -X POST http://localhost:8090/api/orders -H 'Content-Type: application/json' -d '{...}'"
    echo "  - Stop event generation: docker-compose stop event-generator"
}

# Check if required tools are available
check_dependencies() {
    for cmd in docker curl jq; do
        if ! command -v $cmd &> /dev/null; then
            echo -e "${RED}Error: $cmd is required but not installed${NC}"
            exit 1
        fi
    done
}

# Run dependency check and main function
check_dependencies
main
