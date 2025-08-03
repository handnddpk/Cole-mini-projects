#!/bin/bash

# Kafka Microservices Demo Script
set -e

echo "🎬 Starting Kafka E-commerce Microservices Demo..."
echo "================================================="

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
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

# Wait for services to be ready
wait_for_services() {
    print_step "Waiting for all services to be ready..."
    
    services=(
        "http://localhost:8090/actuator/health|Order Service"
        "http://localhost:8091/actuator/health|Inventory Service"
        "http://localhost:8092/actuator/health|Payment Service"
        "http://localhost:8093/actuator/health|Shipping Service"
        "http://localhost:8094/actuator/health|Analytics Service"
    )
    
    for service in "${services[@]}"; do
        url=$(echo $service | cut -d'|' -f1)
        name=$(echo $service | cut -d'|' -f2)
        
        echo -n "Waiting for $name..."
        while ! curl -s -f "$url" > /dev/null; do
            echo -n "."
            sleep 2
        done
        echo " ✓"
    done
    
    print_success "All services are ready!"
    sleep 2
}

# Demo 1: Basic Order Flow
demo_basic_order_flow() {
    print_step "Demo 1: Basic Order Creation Flow"
    echo "=================================="
    
    print_info "Creating a new order..."
    ORDER_RESPONSE=$(curl -s -X POST http://localhost:8090/api/orders \
        -H 'Content-Type: application/json' \
        -d '{
            "customerId": "cust-001",
            "items": [
                {
                    "productId": "prod-001",
                    "productName": "MacBook Pro 16\"",
                    "quantity": 1,
                    "price": 2499.99
                }
            ],
            "shippingAddress": {
                "street": "123 Main St",
                "city": "New York",
                "zipCode": "10001",
                "country": "USA"
            }
        }')
    
    ORDER_ID=$(echo "$ORDER_RESPONSE" | jq -r '.orderId')
    print_success "Order created: $ORDER_ID"
    
    sleep 3
    
    print_info "Checking inventory reservation..."
    curl -s "http://localhost:8091/api/inventory/prod-001" | jq '.'
    
    sleep 2
    
    print_info "Processing payment..."
    curl -s -X POST "http://localhost:8092/api/payments" \
        -H 'Content-Type: application/json' \
        -d "{
            \"orderId\": \"$ORDER_ID\",
            \"amount\": 2499.99,
            \"currency\": \"USD\",
            \"paymentMethod\": \"credit_card\"
        }" | jq '.'
    
    sleep 3
    
    print_info "Checking shipment creation..."
    curl -s "http://localhost:8093/api/shipments/order/$ORDER_ID" | jq '.'
    
    print_success "Basic order flow completed! ✨"
    echo ""
}

# Demo 2: Multiple Orders with Inventory Management
demo_inventory_management() {
    print_step "Demo 2: Inventory Management & Low Stock Alerts"
    echo "=============================================="
    
    print_info "Current inventory levels:"
    curl -s "http://localhost:8091/api/inventory" | jq -r '.[] | "\(.productName): \(.quantity - .reservedQuantity) available"'
    
    echo ""
    print_info "Creating multiple orders to trigger low stock..."
    
    for i in {1..3}; do
        ORDER_RESPONSE=$(curl -s -X POST http://localhost:8090/api/orders \
            -H 'Content-Type: application/json' \
            -d "{
                \"customerId\": \"cust-00$i\",
                \"items\": [
                    {
                        \"productId\": \"prod-003\",
                        \"productName\": \"AirPods Pro\",
                        \"quantity\": 50,
                        \"price\": 249.99
                    }
                ]
            }")
        
        ORDER_ID=$(echo "$ORDER_RESPONSE" | jq -r '.orderId')
        print_info "Created order $i: $ORDER_ID"
        sleep 1
    done
    
    sleep 5
    
    print_info "Updated inventory levels:"
    curl -s "http://localhost:8091/api/inventory/prod-003" | jq '.'
    
    print_info "Checking for low stock alerts..."
    curl -s "http://localhost:8091/api/inventory/low-stock" | jq '.'
    
    print_success "Inventory management demo completed! 📦"
    echo ""
}

# Demo 3: Payment Failures and Order Cancellation
demo_payment_failure() {
    print_step "Demo 3: Payment Failure Handling"
    echo "================================"
    
    print_info "Creating order with insufficient funds simulation..."
    ORDER_RESPONSE=$(curl -s -X POST http://localhost:8090/api/orders \
        -H 'Content-Type: application/json' \
        -d '{
            "customerId": "cust-failed",
            "items": [
                {
                    "productId": "prod-001",
                    "productName": "MacBook Pro 16\"",
                    "quantity": 1,
                    "price": 2499.99
                }
            ]
        }')
    
    ORDER_ID=$(echo "$ORDER_RESPONSE" | jq -r '.orderId')
    print_info "Order created: $ORDER_ID"
    
    sleep 2
    
    print_info "Simulating payment failure..."
    curl -s -X POST "http://localhost:8092/api/payments/$ORDER_ID/fail" \
        -H 'Content-Type: application/json' \
        -d '{"reason": "Insufficient funds"}'
    
    sleep 2
    
    print_info "Checking order status after payment failure..."
    curl -s "http://localhost:8090/api/orders/$ORDER_ID" | jq '.status'
    
    print_success "Payment failure handling demonstrated! 💳"
    echo ""
}

# Demo 4: Real-time Analytics
demo_analytics() {
    print_step "Demo 4: Real-time Analytics Dashboard"
    echo "===================================="
    
    print_info "Current analytics summary:"
    curl -s "http://localhost:8094/api/analytics/summary" | jq '.'
    
    print_info "Order metrics by status:"
    curl -s "http://localhost:8094/api/analytics/orders/by-status" | jq '.'
    
    print_info "Revenue metrics:"
    curl -s "http://localhost:8094/api/analytics/revenue/today" | jq '.'
    
    print_info "Top products:"
    curl -s "http://localhost:8094/api/analytics/products/top" | jq '.'
    
    print_success "Analytics demo completed! 📊"
    echo ""
}

# Demo 5: Event Streaming Visualization
demo_event_streaming() {
    print_step "Demo 5: Event Streaming Visualization"
    echo "====================================="
    
    print_info "Open these URLs to see real-time data:"
    echo "• Kafka UI (Topics & Messages): http://localhost:8080"
    echo "• Grafana Dashboard: http://localhost:3000"
    echo "• Prometheus Metrics: http://localhost:9090"
    echo ""
    
    print_info "Creating rapid order sequence to show event flow..."
    
    for i in {1..5}; do
        CUSTOMER_ID=$(printf "cust-%03d" $((RANDOM % 100 + 1)))
        PRODUCT_ID=$(printf "prod-%03d" $((RANDOM % 5 + 1)))
        QUANTITY=$((RANDOM % 3 + 1))
        PRICE=$(echo "scale=2; ($RANDOM % 1000 + 100) / 1" | bc)
        
        curl -s -X POST http://localhost:8090/api/orders \
            -H 'Content-Type: application/json' \
            -d "{
                \"customerId\": \"$CUSTOMER_ID\",
                \"items\": [
                    {
                        \"productId\": \"$PRODUCT_ID\",
                        \"productName\": \"Product $PRODUCT_ID\",
                        \"quantity\": $QUANTITY,
                        \"price\": $PRICE
                    }
                ]
            }" > /dev/null
        
        print_info "Order $i created - Customer: $CUSTOMER_ID, Product: $PRODUCT_ID"
        sleep 1
    done
    
    print_success "Event streaming demo completed! 🌊"
    echo ""
}

# Show final dashboard
show_dashboard() {
    print_step "📊 Access Your Real-time Dashboards"
    echo "===================================="
    echo ""
    echo "🎯 Service Endpoints:"
    echo "• Order Service API: http://localhost:8090/api/orders"
    echo "• Inventory Service: http://localhost:8091/api/inventory"
    echo "• Payment Service: http://localhost:8092/api/payments"
    echo "• Shipping Service: http://localhost:8093/api/shipments"
    echo "• Analytics Service: http://localhost:8094/api/analytics"
    echo ""
    echo "📱 Monitoring & Visualization:"
    echo "• Kafka UI: http://localhost:8080"
    echo "• Grafana Dashboard: http://localhost:3000 (admin/admin123)"
    echo "• Prometheus Metrics: http://localhost:9090"
    echo ""
    echo "🔍 Try These API Calls:"
    echo "• curl http://localhost:8090/api/orders"
    echo "• curl http://localhost:8091/api/inventory/low-stock"
    echo "• curl http://localhost:8094/api/analytics/summary"
    echo ""
    echo "🎉 Demo completed! Your Kafka microservices are running."
    echo "   Check the Kafka UI to see real-time event streaming!"
}

# Main demo execution
main() {
    wait_for_services
    demo_basic_order_flow
    demo_inventory_management
    demo_payment_failure
    demo_analytics
    demo_event_streaming
    show_dashboard
}

# Check if required tools are available
if ! command -v curl &> /dev/null; then
    echo "curl is required for this demo"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "jq is required for JSON parsing in this demo"
    echo "Install with: brew install jq (macOS) or apt-get install jq (Ubuntu)"
    exit 1
fi

# Run the demo
main "$@"
