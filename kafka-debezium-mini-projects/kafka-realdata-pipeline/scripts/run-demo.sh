#!/bin/bash

# Real-time Data Pipeline Demo Script
set -e

echo "🌊 Starting Kafka Real-time Data Integration Demo..."
echo "==================================================="

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_step() {
    echo -e "${BLUE}[DEMO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if services are running
check_services() {
    print_step "Checking service availability..."
    
    services=(
        "http://localhost:9021|Control Center"
        "http://localhost:8083|Kafka Connect"
        "http://localhost:8081|Schema Registry"
        "http://localhost:9200|Elasticsearch"
        "http://localhost:8123|ClickHouse"
    )
    
    for service in "${services[@]}"; do
        url=$(echo $service | cut -d'|' -f1)
        name=$(echo $service | cut -d'|' -f2)
        
        if curl -s -f "$url" > /dev/null 2>&1; then
            print_success "✓ $name is running"
        else
            print_error "✗ $name is not available at $url"
        fi
    done
    echo ""
}

# Show connector status
show_connector_status() {
    print_step "Checking Kafka Connect Connector Status"
    echo "========================================"
    
    if curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
        CONNECTORS=$(curl -s http://localhost:8083/connectors)
        
        if [ "$CONNECTORS" = "[]" ]; then
            print_info "No connectors deployed yet."
            print_info "Run the setup script to deploy connectors."
        else
            print_success "Active connectors:"
            echo "$CONNECTORS" | jq -r '.[]' | while read connector; do
                STATUS=$(curl -s "http://localhost:8083/connectors/$connector/status" | jq -r '.connector.state')
                if [ "$STATUS" = "RUNNING" ]; then
                    print_success "  ✓ $connector: $STATUS"
                else
                    print_error "  ✗ $connector: $STATUS"
                fi
            done
        fi
    else
        print_error "Kafka Connect is not available"
    fi
    echo ""
}

# Simulate database changes
simulate_database_changes() {
    print_step "Simulating Database Changes for CDC"
    echo "==================================="
    
    print_info "Inserting new orders in MySQL..."
    
    # Connect to MySQL and insert some orders
    docker exec kafka-real-mysql-source mysql -u root -pdebezium -e "
        USE ecommerce;
        INSERT INTO orders (order_id, customer_id, product_name, quantity, price, status) VALUES
        ('order-$(date +%s)-001', 'cust-demo-001', 'Demo Product A', 2, 299.99, 'PENDING'),
        ('order-$(date +%s)-002', 'cust-demo-002', 'Demo Product B', 1, 499.99, 'CONFIRMED'),
        ('order-$(date +%s)-003', 'cust-demo-003', 'Demo Product C', 3, 149.99, 'PENDING');
        
        SELECT 'Recent orders:' as '';
        SELECT order_id, customer_id, product_name, status, created_at FROM orders ORDER BY created_at DESC LIMIT 5;
    " 2>/dev/null || print_error "Failed to insert MySQL data"
    
    sleep 2
    
    print_info "Updating inventory in PostgreSQL..."
    
    # Connect to PostgreSQL and update inventory
    docker exec kafka-real-postgres-source psql -U postgres -d inventory -c "
        UPDATE inventory_items SET quantity = quantity - 5 WHERE product_id = 'prod-001';
        UPDATE inventory_items SET quantity = quantity - 2 WHERE product_id = 'prod-002';
        
        SELECT 'Updated inventory:' as message;
        SELECT product_id, product_name, quantity, reserved_quantity FROM inventory_items WHERE product_id IN ('prod-001', 'prod-002');
    " 2>/dev/null || print_error "Failed to update PostgreSQL data"
    
    sleep 2
    
    print_info "Adding user profile in MongoDB..."
    
    # Connect to MongoDB and insert user profile
    docker exec kafka-real-mongodb-source mongosh --eval "
        use userprofiles;
        db.user_profiles.insertOne({
            user_id: 'user-demo-' + Date.now(),
            customer_id: 'cust-demo-001',
            preferences: {
                categories: ['Electronics', 'Books'],
                brands: ['Apple', 'Samsung'],
                price_range: { min: 100, max: 2000 },
                notifications: { email: true, sms: false, push: true }
            },
            demographics: {
                age_group: '25-34',
                location: { city: 'Demo City', state: 'DC', country: 'USA' },
                income_bracket: 'medium'
            },
            behavior: {
                avg_order_value: 425.50,
                order_frequency: 'monthly',
                last_login: new Date(),
                favorite_payment_method: 'credit_card'
            },
            created_at: new Date(),
            updated_at: new Date()
        });
        
        print('User profiles count:', db.user_profiles.countDocuments());
    " 2>/dev/null || print_error "Failed to insert MongoDB data"
    
    print_success "Database changes simulated! ✨"
    echo ""
}

# Check Kafka topics
show_kafka_topics() {
    print_step "Checking Kafka Topics and Messages"
    echo "=================================="
    
    if command -v kafka-topics.sh &> /dev/null; then
        print_info "Available topics:"
        kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E "(cdc\.|financial|weather|ecommerce)" | sort
    else
        print_info "Using Docker to check topics:"
        docker exec kafka-real-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -E "(cdc\.|financial|weather|ecommerce)" | sort || print_info "No CDC topics found yet"
    fi
    echo ""
}

# Query target systems
query_target_systems() {
    print_step "Querying Target Systems for Data"
    echo "==============================="
    
    print_info "Querying Elasticsearch for recent data..."
    if curl -s "http://localhost:9200/_cat/indices?v" | grep -q cdc; then
        curl -s "http://localhost:9200/cdc*/_search?size=3&sort=@timestamp:desc" | jq '.hits.hits[]._source' 2>/dev/null || print_info "No data found in Elasticsearch yet"
    else
        print_info "No CDC indices found in Elasticsearch yet"
    fi
    
    echo ""
    
    print_info "Querying ClickHouse for analytics data..."
    if docker exec kafka-real-clickhouse clickhouse-client --query "SHOW TABLES" 2>/dev/null | grep -q kafka; then
        docker exec kafka-real-clickhouse clickhouse-client --query "
            SELECT 'Recent data count by topic:' as message;
            SELECT kafka_topic, count() as record_count 
            FROM kafka_connect.kafka_data 
            GROUP BY kafka_topic 
            ORDER BY record_count DESC 
            LIMIT 5;
        " 2>/dev/null || print_info "No data found in ClickHouse yet"
    else
        print_info "No tables found in ClickHouse yet"
    fi
    
    echo ""
}

# Generate synthetic API data
generate_api_data() {
    print_step "Generating Synthetic API Data"
    echo "============================="
    
    if docker ps | grep -q api-collector; then
        print_success "API collector is running and generating data"
        print_info "Check these topics for API data:"
        echo "  • financial-data (stock prices)"
        echo "  • weather-data (weather information)"
        echo "  • ecommerce-orders (simulated orders)"
    else
        print_info "API collector not running. Starting manual data generation..."
        
        # Generate some sample financial data
        print_info "Generating sample financial data..."
        
        for symbol in AAPL GOOGL MSFT TSLA AMZN; do
            PRICE=$(echo "scale=2; ($RANDOM % 50000 + 10000) / 100" | bc)
            CHANGE=$(echo "scale=2; ($RANDOM % 1000 - 500) / 100" | bc)
            
            DATA="{
                \"symbol\": \"$symbol\",
                \"price\": $PRICE,
                \"change\": $CHANGE,
                \"changePercent\": \"$(echo "scale=2; $CHANGE / $PRICE * 100" | bc)%\",
                \"volume\": $((RANDOM * 1000000)),
                \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%S)\",
                \"source\": \"demo-generator\"
            }"
            
            echo "$DATA" | docker exec -i kafka-real-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic financial-data 2>/dev/null || true
            print_info "  Generated data for $symbol: \$$PRICE"
        done
        
        # Generate sample weather data
        print_info "Generating sample weather data..."
        
        CITIES=("London" "NewYork" "Tokyo" "Sydney" "Mumbai")
        for city in "${CITIES[@]}"; do
            TEMP=$((RANDOM % 40 - 5))  # -5 to 35°C
            HUMIDITY=$((RANDOM % 50 + 30))  # 30-80%
            
            DATA="{
                \"city\": \"$city\",
                \"country\": \"Demo\",
                \"temperature\": $TEMP,
                \"humidity\": $HUMIDITY,
                \"pressure\": $((RANDOM % 50 + 990)),
                \"description\": \"Demo weather\",
                \"windSpeed\": $(echo "scale=1; ($RANDOM % 200) / 10" | bc),
                \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%S)\",
                \"source\": \"demo-generator\"
            }"
            
            echo "$DATA" | docker exec -i kafka-real-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic weather-data 2>/dev/null || true
            print_info "  Generated weather for $city: ${TEMP}°C"
        done
    fi
    
    print_success "API data generation completed! 🌐"
    echo ""
}

# Show monitoring dashboards
show_monitoring() {
    print_step "Monitoring & Visualization Access"
    echo "================================"
    
    echo "📊 Open these URLs to monitor your pipeline:"
    echo ""
    echo "🎛️  Control Center (Kafka Management):"
    echo "   http://localhost:9021"
    echo ""
    echo "🔗 Kafka Connect (Connector Management):"
    echo "   http://localhost:8083/connectors"
    echo ""
    echo "🔍 Elasticsearch (Search & Analytics):"
    echo "   http://localhost:9200/_cat/indices?v"
    echo ""
    echo "📈 ClickHouse (Analytics Database):"
    echo "   Connect via: docker exec -it kafka-real-clickhouse clickhouse-client"
    echo ""
    echo "📊 Grafana Dashboards:"
    echo "   http://localhost:3000 (admin/admin123)"
    echo ""
    echo "🎯 Quick Health Checks:"
    echo "   • curl http://localhost:8083/connectors"
    echo "   • curl http://localhost:9200/_cat/health"
    echo "   • docker exec kafka-real-kafka kafka-topics --bootstrap-server localhost:9092 --list"
    echo ""
}

# Main demo execution
main() {
    echo "🚀 Real-time Data Integration Pipeline Demo"
    echo "=========================================="
    echo ""
    
    check_services
    show_connector_status
    simulate_database_changes
    show_kafka_topics
    generate_api_data
    query_target_systems
    show_monitoring
    
    print_success "🎉 Demo completed!"
    print_info "Your real-time data pipeline is processing data from multiple sources."
    print_info "Check the Control Center and target systems to see the data flowing!"
}

# Check prerequisites
if ! command -v curl &> /dev/null; then
    print_error "curl is required for this demo"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    print_error "jq is required for JSON processing"
    print_info "Install with: brew install jq (macOS) or apt-get install jq (Ubuntu)"
    exit 1
fi

if ! command -v bc &> /dev/null; then
    print_error "bc is required for calculations"
    print_info "Install with: brew install bc (macOS) or apt-get install bc (Ubuntu)"
    exit 1
fi

# Run the demo
main "$@"
