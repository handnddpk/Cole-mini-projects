# HDFS Metadata Management on Object Storage (MinIO)

## Project Description

This project simulates HDFS-like metadata management on top of MinIO object storage, demonstrating the fundamental trade-offs between traditional distributed filesystems and object storage systems. Students will build a metadata service that provides HDFS semantics while leveraging object storage's infinite scalability, learning about the performance and consistency challenges that arise when bridging these two paradigms.

## Key Learning Objectives

- **Understand Storage Trade-offs**: Experience the differences between HDFS (fast metadata ops, heap-limited) vs Object Storage (slower individual ops, unlimited scale)
- **Metadata Architecture**: Design scalable metadata services for billions of objects without NameNode heap limitations
- **Consistency Challenges**: Handle eventual consistency, rename atomicity, and directory semantics on object storage
- **Performance Optimization**: Implement caching and batching strategies to mitigate object storage latency
- **Distributed Systems**: Build fault-tolerant services with proper failure handling and recovery

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    HDFS-on-MinIO Architecture                       │
├─────────────────────────────────────────────────────────────────────┤
│  HDFS Client API     │    Metadata Service     │   MinIO Cluster    │
│  ├─ create()         │    ├─ Namespace Mgmt    │   ├─ Object Store  │
│  ├─ open()           │    ├─ Block Mapping     │   ├─ Erasure Code  │
│  ├─ delete()         │    ├─ Consistency Layer │   ├─ Auto-scaling  │
│  ├─ rename()         │    └─ Cache Layer       │   └─ Replication   │
│  └─ listStatus()     │                         │                    │
├─────────────────────────────────────────────────────────────────────┤
│  PostgreSQL          │    Redis Cache          │   Load Balancer    │
│  ├─ Namespace Tree   │    ├─ Hot Metadata      │   ├─ Service HA    │
│  ├─ Block Metadata   │    ├─ Directory Cache   │   ├─ Health Check  │
│  ├─ Transactions     │    └─ Negative Cache    │   └─ Failover      │
│  └─ ACID Guarantees  │                         │                    │
└─────────────────────────────────────────────────────────────────────┘
```

## Core Technical Challenges

### Challenge 1: Atomic Rename Operations
- **HDFS**: Single atomic operation in NameNode
- **Object Storage**: Requires copy + delete (not atomic)
- **Solution**: Implement distributed transactions with rollback capability

### Challenge 2: Directory Listing Performance  
- **HDFS**: O(1) directory listing from in-memory structures
- **Object Storage**: Expensive prefix scans across distributed objects
- **Solution**: Materialized directory views with smart caching

### Challenge 3: Small File Efficiency
- **HDFS**: Block-based storage optimized for large files
- **Object Storage**: High per-request overhead for small objects
- **Solution**: Intelligent object aggregation and lazy materialization

### Challenge 4: Consistency Guarantees
- **HDFS**: Strong consistency with immediate read-after-write
- **Object Storage**: Eventual consistency model
- **Solution**: Consistency layer with conflict resolution

## Implementation Requirements

### Core Components to Build

1. **Metadata Service** (`MetadataService.java`)
   - RESTful API compatible with HDFS WebHDFS
   - Namespace management (create, delete, rename, list)
   - Block-to-object mapping logic
   - Distributed transaction handling

2. **Storage Abstraction** (`ObjectStorageAdapter.java`)
   - MinIO client wrapper with retry logic
   - Batch operation support
   - Connection pooling and circuit breakers
   - Metadata consistency enforcement

3. **Caching Layer** (`MetadataCache.java`)
   - Multi-level caching (L1: in-memory, L2: Redis)
   - Cache invalidation strategies
   - Negative caching for non-existent paths
   - Cache coherence across service instances

4. **Database Schema** (PostgreSQL)
   - Namespace tree representation
   - Block metadata and locations
   - Transaction logs for consistency
   - Optimized indexes for common queries

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Java 11+ JDK
- Maven 3.8+
- 4GB+ RAM available

### Quick Start

```bash
# Clone and navigate to project
cd hadoop-deep-technical

# Start the complete stack
docker-compose up -d

# Wait for services to be ready
./scripts/wait-for-services.sh

# Initialize database schema
./scripts/init-metadata-db.sh

# Run basic functionality tests
./scripts/test-basic-operations.sh

# Access the services
open http://localhost:8080/api/v1/health    # Metadata Service
open http://localhost:9001                  # MinIO Console  
open http://localhost:3000                  # Grafana Dashboard
```

### Service Endpoints

- **Metadata Service API**: `http://localhost:8080`
- **HDFS WebHDFS Compatible**: `http://localhost:8080/webhdfs/v1`
- **MinIO Console**: `http://localhost:9001` (admin/minioadmin123)
- **PostgreSQL**: `localhost:5432` (hdfs_admin/hdfs_password)
- **Redis Cache**: `localhost:6379`
- **Monitoring Dashboard**: `http://localhost:3000` (admin/admin123)

## Performance Comparison Demonstrations

The project includes benchmarks that demonstrate key differences:

### Metadata Operation Latency
```bash
./scripts/benchmark-metadata-ops.sh
```
- HDFS-style operations: ~1-5ms (in-memory)
- Object storage operations: ~50-200ms (network + consistency)
- Cached operations: ~1-10ms (cache hit)

### Directory Listing Performance
```bash
./scripts/benchmark-directory-listing.sh
```
- Small directories (<100 files): Similar performance
- Large directories (10K+ files): Object storage 10x slower
- Cached listings: Near HDFS performance

### Rename Operation Cost
```bash
./scripts/benchmark-rename-ops.sh
```
- HDFS rename: O(1) - single metadata update
- Object storage rename: O(n) - copy all objects + metadata updates
- Large directory rename: Can take minutes vs milliseconds

## Learning Exercises

### Exercise 1: Basic HDFS Operations
Implement and test basic file operations:
```bash
# Create directory
curl -X PUT "http://localhost:8080/webhdfs/v1/user/test?op=MKDIRS"

# Upload file  
curl -X PUT "http://localhost:8080/webhdfs/v1/user/test/file1.txt?op=CREATE" \
     -H "Content-Type: application/octet-stream" \
     --data "Hello MinIO HDFS!"

# List directory
curl "http://localhost:8080/webhdfs/v1/user/test?op=LISTSTATUS"

# Download file
curl "http://localhost:8080/webhdfs/v1/user/test/file1.txt?op=OPEN"
```

### Exercise 2: Performance Analysis
Compare operation costs:
```bash
# Measure metadata operation latency
./scripts/measure-latency.sh

# Analyze cache hit ratios
./scripts/analyze-cache-performance.sh

# Profile memory usage vs object count
./scripts/profile-memory-usage.sh
```

### Exercise 3: Failure Scenarios
Test system resilience:
```bash
# Simulate MinIO node failure
docker-compose stop minio1

# Simulate metadata service crash  
docker-compose restart metadata-service-1

# Test split-brain scenarios
./scripts/test-split-brain.sh
```

## Development Workflow

### Building the Project
```bash
# Compile Java components
mvn clean compile

# Run unit tests
mvn test

# Build Docker images
docker-compose build

# Run integration tests
mvn verify -P integration-tests
```

### Code Structure
```
src/main/java/org/custom/hdfs/
├── metadata/           # Core metadata service
│   ├── MetadataService.java
│   ├── NamespaceManager.java  
│   └── BlockManager.java
├── storage/            # Object storage abstraction
│   ├── ObjectStorageAdapter.java
│   ├── MinIOClient.java
│   └── ConsistencyManager.java
├── cache/              # Caching implementation
│   ├── MetadataCache.java
│   ├── RedisCache.java
│   └── CacheCoherence.java
└── api/                # REST API controllers
    ├── WebHDFS.java
    └── HealthCheck.java
```

## Expected Learning Outcomes

After completing this project, students will understand:

1. **Storage Architecture Trade-offs**
   - Memory-bound vs. storage-bound systems
   - Consistency vs. availability trade-offs
   - Performance vs. scalability considerations

2. **Distributed Systems Design**
   - Caching strategies and cache invalidation
   - Failure detection and recovery
   - Load balancing and service discovery

3. **Metadata Management at Scale**
   - Hierarchical namespace representation
   - Efficient directory operations
   - Block-to-object mapping strategies

4. **Performance Engineering**
   - Identifying bottlenecks in distributed systems
   - Optimization through caching and batching
   - Monitoring and alerting for production systems

## Monitoring and Observability

The project includes comprehensive monitoring:

- **Metrics**: Prometheus metrics for all operations
- **Dashboards**: Grafana dashboards for visualization  
- **Tracing**: Distributed tracing with Jaeger
- **Logging**: Structured logging with correlation IDs
- **Alerting**: Prometheus alerts for system health

Key metrics to monitor:
- Metadata operation latency (p50, p95, p99)
- Cache hit ratios and eviction rates
- Object storage operation costs
- Database connection pool utilization
- Memory usage and GC behavior

This project provides hands-on experience with the challenges of building scalable metadata services, making it an excellent educational tool for understanding distributed storage systems.
