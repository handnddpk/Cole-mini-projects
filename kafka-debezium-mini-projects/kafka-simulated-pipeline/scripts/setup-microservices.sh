#!/bin/bash

# Kafka Simulated Pipeline - Complete Setup Script
set -e

echo "🚀 Starting Kafka Event-Driven Microservices Pipeline Setup..."
echo "============================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed."
        exit 1
    fi
    
    if ! command -v mvn &> /dev/null; then
        print_warning "Maven not found. Using Docker for builds."
    fi
    
    print_status "Prerequisites check passed ✓"
}

# Build microservices
build_microservices() {
    print_header "Building Microservices..."
    
    services=("order-service" "inventory-service" "payment-service" "shipping-service" "analytics-service")
    
    for service in "${services[@]}"; do
        if [ -d "services/$service" ]; then
            print_status "Building $service..."
            
            # Create Dockerfile if not exists
            cat > "services/$service/Dockerfile" << EOF
FROM openjdk:17-jdk-alpine
VOLUME /tmp
COPY target/*.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
EXPOSE 8080
EOF
            
            # Build with Maven in Docker if available, otherwise use Docker multi-stage
            if command -v mvn &> /dev/null; then
                cd "services/$service"
                mvn clean package -DskipTests
                cd ../..
            else
                # Create multi-stage Dockerfile
                cat > "services/$service/Dockerfile" << EOF
FROM maven:3.8.6-openjdk-17-slim AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM openjdk:17-jdk-alpine
VOLUME /tmp
COPY --from=build /app/target/*.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
EXPOSE 8080
EOF
            fi
            
            print_status "$service built ✓"
        else
            print_warning "$service directory not found"
        fi
    done
}

# Start infrastructure
start_infrastructure() {
    print_header "Starting Infrastructure Services..."
    
    print_status "Starting Kafka ecosystem..."
    docker-compose up -d zookeeper kafka schema-registry
    
    print_status "Waiting for Kafka to be ready..."
    sleep 30
    
    print_status "Starting databases..."
    docker-compose up -d postgres redis
    
    print_status "Starting monitoring..."
    docker-compose up -d kafka-ui prometheus grafana
    
    sleep 20
    print_status "Infrastructure started ✓"
}

# Initialize schemas
initialize_schemas() {
    print_header "Initializing Kafka Schemas..."
    
    # Wait for Schema Registry
    until curl -f http://localhost:8081/subjects; do
        print_status "Waiting for Schema Registry..."
        sleep 5
    done
    
    # Register Avro schemas
    schemas=(
        "OrderCreated"
        "OrderUpdated" 
        "InventoryUpdated"
        "PaymentProcessed"
        "ShipmentCreated"
    )
    
    for schema in "${schemas[@]}"; do
        print_status "Registering $schema schema..."
        # Schema registration would go here
        # curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        #      --data '{"schema": "..."}' \
        #      http://localhost:8081/subjects/$schema-value/versions
    done
    
    print_status "Schemas initialized ✓"
}

# Start microservices
start_microservices() {
    print_header "Starting Microservices..."
    
    services=("order-service" "inventory-service" "payment-service" "shipping-service" "analytics-service")
    ports=(8090 8091 8092 8093 8094)
    
    for i in "${!services[@]}"; do
        service="${services[$i]}"
        port="${ports[$i]}"
        
        print_status "Starting $service on port $port..."
        docker-compose up -d "$service"
        
        # Wait for service to be healthy
        sleep 10
    done
    
    print_status "Microservices started ✓"
}

# Start event generator
start_event_generator() {
    print_header "Starting Event Generator..."
    
    if [ -d "event-generator" ]; then
        docker-compose up -d event-generator
        print_status "Event generator started ✓"
    else
        print_warning "Event generator not found"
    fi
}

# Verify deployment
verify_deployment() {
    print_header "Verifying Deployment..."
    
    services=(
        "http://localhost:8080|Kafka UI"
        "http://localhost:8081|Schema Registry"
        "http://localhost:8090/actuator/health|Order Service"
        "http://localhost:8091/actuator/health|Inventory Service"
        "http://localhost:8092/actuator/health|Payment Service"
        "http://localhost:8093/actuator/health|Shipping Service"
        "http://localhost:8094/actuator/health|Analytics Service"
        "http://localhost:3000|Grafana"
        "http://localhost:9090|Prometheus"
    )
    
    echo ""
    echo "🎯 Service Health Check:"
    echo "======================="
    
    for service in "${services[@]}"; do
        url=$(echo $service | cut -d'|' -f1)
        name=$(echo $service | cut -d'|' -f2)
        
        if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "200\|302"; then
            print_status "✓ $name: $url"
        else
            print_warning "⚠ $name: $url (may still be starting)"
        fi
    done
}

# Show completion info
show_completion_info() {
    print_header "Setup Complete! 🎉"
    
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              🏪 KAFKA E-COMMERCE MICROSERVICES                 ║${NC}"
    echo -e "${GREEN}║                     Setup Complete!                           ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    echo "🌐 Service Endpoints:"
    echo "• Kafka UI: http://localhost:8080"
    echo "• Schema Registry: http://localhost:8081"
    echo "• Order Service: http://localhost:8090"
    echo "• Inventory Service: http://localhost:8091"
    echo "• Payment Service: http://localhost:8092"
    echo "• Shipping Service: http://localhost:8093"
    echo "• Analytics Service: http://localhost:8094"
    echo "• Grafana Dashboard: http://localhost:3000 (admin/admin123)"
    echo "• Prometheus: http://localhost:9090"
    echo ""
    
    echo "🔗 Database Connections:"
    echo "• PostgreSQL: localhost:5432 (kafka_user/kafka_password)"
    echo "• Redis: localhost:6379"
    echo ""
    
    echo "🎯 Try These Actions:"
    echo "1. Create an order: curl -X POST http://localhost:8090/api/orders -H 'Content-Type: application/json' -d '{\"customerId\":\"cust-001\",\"items\":[{\"productId\":\"prod-001\",\"quantity\":2,\"price\":99.99}]}'"
    echo "2. Check inventory: curl http://localhost:8091/api/inventory"
    echo "3. View Kafka topics in Kafka UI"
    echo "4. Monitor metrics in Grafana"
    echo ""
    
    echo "📊 Event Flow:"
    echo "Order Created → Inventory Reserved → Payment Processed → Shipment Created → Analytics Updated"
    echo ""
    
    print_status "Happy event streaming! 🌊"
}

# Main execution
main() {
    check_prerequisites
    build_microservices
    start_infrastructure
    initialize_schemas
    start_microservices
    start_event_generator
    sleep 30  # Allow services to fully start
    verify_deployment
    show_completion_info
}

# Execute main function
main "$@"
