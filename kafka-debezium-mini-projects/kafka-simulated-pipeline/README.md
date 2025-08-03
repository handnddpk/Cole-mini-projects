# Kafka Event-Driven Microservices Pipeline

## Project Description

This project demonstrates a comprehensive event-driven microservices architecture using Apache Kafka as the central nervous system. It simulates a realistic e-commerce platform with multiple microservices communicating through events, implementing patterns like Event Sourcing, CQRS (Command Query Responsibility Segregation), and Saga patterns for distributed transactions.

## Data Source & Simulation

- **Type**: E-commerce Business Event Simulation
- **Domains**: Order Management, Inventory, Payment, Shipping, Customer Service, Analytics
- **Format**: Apache Avro with Schema Registry for schema evolution
- **Volume**: 10K+ events/minute across all services
- **Event Types**:
  - **Order Events**: OrderCreated, OrderConfirmed, OrderCancelled, OrderShipped, OrderDelivered
  - **Inventory Events**: StockUpdated, LowStockAlert, RestockRequested, ProductAdded
  - **Payment Events**: PaymentInitiated, PaymentProcessed, PaymentFailed, RefundIssued
  - **Customer Events**: CustomerRegistered, CustomerProfileUpdated, CustomerPreferencesChanged
  - **Shipping Events**: ShipmentCreated, ShipmentInTransit, ShipmentDelivered, DeliveryFailed

## Tech Stack

- **Messaging Platform**: Apache Kafka 3.7 with Schema Registry
- **Stream Processing**: Kafka Streams API for real-time event processing
- **Microservices Framework**: Spring Boot 3.2 with Spring Kafka
- **Database**: PostgreSQL (event store) + Redis (caching)
- **Container Platform**: Docker & Docker Compose
- **Monitoring**: Kafka UI, Spring Boot Actuator, Prometheus, Grafana
- **Testing**: Testcontainers for integration testing
- **Languages**: Java 17 (microservices), Python (data generation)

## Project Purpose

- Master Apache Kafka as a distributed streaming platform
- Learn event-driven architecture patterns and best practices
- Understand microservices communication through events
- Practice Event Sourcing and CQRS implementation
- Explore Kafka Streams for real-time stream processing
- Learn schema evolution with Apache Avro and Schema Registry
- Implement distributed transaction patterns (Saga pattern)
- Practice monitoring and observability in distributed systems

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- Java 17+ and Maven 3.8+ (for local development)
- Python 3.8+ (for event simulation)
- At least 4GB RAM available for containers
- Basic understanding of microservices and event-driven patterns

### Quick Start

```bash
# Clone and navigate to project
cd kafka-simulated-pipeline

# Start the complete ecosystem
docker-compose up -d

# Wait for all services to be ready
./scripts/wait-for-services.sh

# Initialize schemas in Schema Registry
./scripts/setup-schemas.sh

# Deploy microservices
./scripts/deploy-microservices.sh

# Start event simulation
./scripts/start-event-simulation.sh

# Access the services
open http://localhost:8080    # Kafka UI
open http://localhost:8081    # Schema Registry UI
open http://localhost:3000    # Grafana Dashboard
```

### Service Endpoints

- **Kafka UI**: `http://localhost:8080`
- **Schema Registry**: `http://localhost:8081`
- **Order Service**: `http://localhost:8090/orders`
- **Inventory Service**: `http://localhost:8091/inventory`
- **Payment Service**: `http://localhost:8092/payments`
- **Shipping Service**: `http://localhost:8093/shipping`
- **Analytics Dashboard**: `http://localhost:8094/analytics`
- **Grafana Dashboard**: `http://localhost:3000` (admin/admin123)
- **PostgreSQL**: `localhost:5432` (kafka_user/kafka_password)

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                    Event-Driven Microservices Architecture              │
├─────────────────────────────────────────────────────────────────────────┤
│  Event Producers       │  Kafka Cluster        │  Event Consumers       │
│  ├─ Order Service      │  ├─ Topic: orders     │  ├─ Inventory Service  │
│  ├─ Payment Service    │  ├─ Topic: payments   │  ├─ Shipping Service   │
│  ├─ Customer Service   │  ├─ Topic: inventory  │  ├─ Analytics Service  │
│  └─ External APIs      │  └─ Schema Registry   │  └─ Notification Svc   │
├─────────────────────────────────────────────────────────────────────────┤
│  Stream Processing     │  Storage Layer        │  Monitoring            │
│  ├─ Kafka Streams      │  ├─ PostgreSQL        │  ├─ Kafka UI           │
│  ├─ Event Aggregation  │  ├─ Redis Cache       │  ├─ Prometheus         │
│  ├─ Real-time Analytics│  └─ Event Store       │  └─ Grafana            │
│  └─ Saga Coordination  │                       │                        │
└─────────────────────────────────────────────────────────────────────────┘
```

## Event Flow Patterns

### 1. Order Processing Saga
```text
Customer Order → Order Created → Inventory Check → Payment Process → Shipping
     ↓              ↓               ↓                 ↓              ↓
 Order Service → Inventory Svc → Payment Svc → Shipping Svc → Customer Notify
```

### 2. Event Sourcing Pattern
```text
Command → Event Store → Event Stream → Read Models → Query APIs
   ↓         ↓            ↓             ↓             ↓
Business   PostgreSQL   Kafka        Materialized   REST/GraphQL
 Logic     Events       Topics         Views        Endpoints
```

### 3. CQRS Implementation
```text
Write Side (Commands)     │     Read Side (Queries)
    ↓                     │         ↓
Command Handlers          │     Query Handlers
    ↓                     │         ↓
Event Store              │     Read Models
    ↓                     │         ↓
Event Stream    ────────────────→  Projections
```

## Microservices Detail

### Order Service (Port 8090)
- **Responsibilities**: Order lifecycle management, order validation
- **Events Produced**: OrderCreated, OrderUpdated, OrderCancelled
- **Events Consumed**: PaymentProcessed, InventoryReserved, ShipmentCreated
- **Database**: PostgreSQL (order events)
- **APIs**: REST endpoints for order management

### Inventory Service (Port 8091)
- **Responsibilities**: Stock management, inventory tracking
- **Events Produced**: StockUpdated, LowStockAlert, InventoryReserved
- **Events Consumed**: OrderCreated, OrderCancelled, ProductAdded
- **Database**: PostgreSQL (inventory state)
- **Features**: Real-time stock levels, automatic reordering

### Payment Service (Port 8092)
- **Responsibilities**: Payment processing, refund handling
- **Events Produced**: PaymentProcessed, PaymentFailed, RefundIssued
- **Events Consumed**: OrderCreated, OrderCancelled
- **Integration**: Mock payment gateway simulation
- **Security**: PCI compliance patterns (simulated)

### Shipping Service (Port 8093)
- **Responsibilities**: Shipment management, delivery tracking
- **Events Produced**: ShipmentCreated, ShipmentInTransit, ShipmentDelivered
- **Events Consumed**: OrderConfirmed, PaymentProcessed
- **Integration**: Mock shipping provider APIs
- **Features**: Real-time tracking, delivery notifications

### Analytics Service (Port 8094)
- **Responsibilities**: Real-time analytics, business intelligence
- **Events Consumed**: All domain events
- **Processing**: Kafka Streams for aggregations
- **Output**: Real-time dashboards, metrics, alerts
- **Storage**: Time-series data in PostgreSQL

## Event Schema Examples

### Order Event Schema (Avro)
```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.ecommerce.events",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "items", "type": {"type": "array", "items": "OrderItem"}},
    {"name": "totalAmount", "type": "double"},
    {"name": "timestamp", "type": "long"},
    {"name": "metadata", "type": {"type": "map", "values": "string"}}
  ]
}
```

### Payment Event Schema (Avro)
```json
{
  "type": "record",
  "name": "PaymentProcessed",
  "namespace": "com.ecommerce.events",
  "fields": [
    {"name": "paymentId", "type": "string"},
    {"name": "orderId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "status", "type": {"type": "enum", "symbols": ["SUCCESS", "FAILED", "PENDING"]}},
    {"name": "timestamp", "type": "long"}
  ]
}
```

## Stream Processing Examples

### Real-time Order Analytics
```java
KStream<String, OrderCreated> orders = builder.stream("orders");
KTable<String, Long> orderCounts = orders
    .groupBy((key, value) -> value.getCustomerId())
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
    .count();
```

### Inventory Low Stock Alerts
```java
KStream<String, InventoryUpdated> inventory = builder.stream("inventory");
inventory
    .filter((key, value) -> value.getQuantity() < value.getThreshold())
    .to("low-stock-alerts");
```

## Performance & Monitoring

### Kafka Configuration
- **Brokers**: 3 replicas for high availability
- **Partitions**: Auto-scaled based on throughput
- **Replication Factor**: 3 for critical topics
- **Retention**: 7 days for event replay capability

### Metrics & Observability
- **Kafka Metrics**: Topic throughput, consumer lag, broker health
- **Application Metrics**: Service response times, error rates, business KPIs
- **Custom Dashboards**: Order processing pipeline, inventory levels, payment success rates
- **Alerting**: Critical event processing delays, service failures, schema evolution issues

## Testing Strategy

### Unit Testing
- Service logic with mocked Kafka producers/consumers
- Event schema validation
- Business rule validation

### Integration Testing
- End-to-end event flow testing with Testcontainers
- Schema evolution compatibility testing
- Saga pattern transaction testing

### Performance Testing
- Load testing with high event volumes
- Consumer lag monitoring under stress
- Throughput benchmarking

## Future Expansion Directions

### 1. Advanced Event Processing
- **Event Sourcing Frameworks**: Implement Axon Framework or EventStore
- **Complex Event Processing**: Add Apache Flink for advanced stream analytics
- **Event Replay & Time Travel**: Build event store query capabilities
- **Event Versioning**: Advanced schema evolution strategies

### 2. Distributed Transaction Patterns
- **Saga Orchestration**: Implement centralized saga coordinator
- **Saga Choreography**: Distributed saga with event-driven coordination
- **Two-Phase Commit**: Implement 2PC for critical transactions
- **Compensating Actions**: Advanced rollback mechanisms

### 3. Production Readiness
- **Kafka Connect Integration**: Real database CDC with Debezium
- **Multi-Region Deployment**: Cross-data center replication
- **Security Implementation**: SASL, SSL, ACLs, encryption at rest
- **Disaster Recovery**: Backup/restore strategies, failover automation

### 4. Advanced Monitoring & Observability
- **Distributed Tracing**: Implement with Jaeger or Zipkin
- **Event Lineage Tracking**: Data lineage across microservices
- **Custom Metrics**: Business-specific KPIs and SLAs
- **Automated Alerting**: ML-based anomaly detection

### 5. UI & Developer Experience
- **Event Explorer UI**: Browse and search events across topics
- **Schema Evolution UI**: Visual schema management
- **Saga Visualization**: Real-time saga execution monitoring
- **Developer Tools**: Event testing, mocking, replay utilities

### 6. Machine Learning Integration
- **Real-time Recommendations**: Customer behavior analysis
- **Fraud Detection**: Payment anomaly detection
- **Demand Forecasting**: Inventory optimization
- **Customer Segmentation**: Real-time customer analytics

### 7. Cloud-Native Enhancements
- **Kubernetes Deployment**: Helm charts, operators
- **Service Mesh Integration**: Istio for service communication
- **GitOps Deployment**: ArgoCD integration
- **Auto-scaling**: HPA based on Kafka lag

## Learning Resources

### Books
- "Designing Event-Driven Systems" by Ben Stopford
- "Building Event-Driven Microservices" by Adam Bellemare
- "Kafka: The Definitive Guide" by Neha Narkhede

### Online Courses
- [Confluent Kafka Fundamentals](https://developer.confluent.io/learn-kafka/)
- [Event-Driven Architecture Patterns](https://www.udemy.com/course/event-driven-microservices/)
- [Microservices with Spring Boot and Spring Cloud](https://www.pluralsight.com/courses/microservices-spring-boot-spring-cloud)

### Documentation & Tutorials
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Reference](https://docs.spring.io/spring-kafka/docs/current/reference/html/)
- [Confluent Platform Documentation](https://docs.confluent.io/)

---

**Master event-driven architecture with real-world patterns! 🚀**
