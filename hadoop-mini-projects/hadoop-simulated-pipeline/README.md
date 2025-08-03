# Hadoop Ecosystem Simulated Data Pipeline

## Project Description
This project demonstrates a comprehensive big data ecosystem using Apache Hadoop with simulated e-commerce and retail data. The pipeline integrates HDFS, Hive, Pig, Sqoop, and MapReduce to create a complete data warehouse solution with ETL processing, SQL analytics, and data integration capabilities.

## Data Source
- **Type**: Multi-domain Simulated Data
- **Domains**: E-commerce transactions, customer profiles, inventory, supplier data, web logs
- **Format**: CSV, JSON, Parquet, MySQL database
- **Volume**: 1M+ transactions, 100K+ customers, 10K+ products
- **Schema**: 
  - **Transactions**: transaction_id, customer_id, product_id, category, price, quantity, timestamp, location, payment_method
  - **Customers**: customer_id, name, email, age, gender, city, state, registration_date, loyalty_tier
  - **Products**: product_id, name, category, subcategory, price, cost, supplier_id, stock_quantity
  - **Suppliers**: supplier_id, name, contact, city, country, rating, contract_date
  - **Web Logs**: session_id, user_id, page, action, timestamp, ip_address, user_agent

## Tech Stack
- **Storage**: Apache Hadoop HDFS 3.3.6
- **Data Warehouse**: Apache Hive 3.1.3
- **ETL Processing**: Apache Pig 0.17.0
- **Data Integration**: Apache Sqoop 1.4.7
- **Processing**: Apache Hadoop MapReduce, Spark SQL
- **Database**: MySQL 8.0 (source system)
- **Container**: Docker & Docker Compose
- **Data Generation**: Python with Faker, SQLAlchemy
- **Monitoring**: Hadoop Web UI, Hive Web UI, Spark UI
- **Languages**: Java (MapReduce), Python (Data Generation), HiveQL, Pig Latin, SQL

## Project Purpose

- Master the complete Hadoop ecosystem and tool integration
- Learn data warehousing concepts with Hive
- Understand ETL processing using Pig Latin
- Practice data integration with Sqoop
- Implement complex multi-stage data pipelines
- Explore SQL analytics on big data
- Learn data modeling and schema design
- Understand batch processing optimization techniques

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available for containers
- Java 8+ (for local development)
- MySQL client (optional, for direct database access)

### Quick Start

```bash
# Clone and navigate to project
cd hadoop-simulated-pipeline

# Start the complete Hadoop ecosystem
docker-compose up -d

# Wait for all services to be ready (check logs)
docker-compose logs -f namenode hive-server sqoop

# Generate comprehensive sample data
docker-compose exec mysql-source bash /scripts/setup_source_database.sh
docker-compose exec data-generator python /scripts/generate_multi_domain_data.py

# Run the complete ETL pipeline
docker-compose exec namenode bash /scripts/run_comprehensive_pipeline.sh

# Access analytics results
docker-compose exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/analytics_queries.sql

# View processed data
docker-compose exec namenode hdfs dfs -ls /warehouse/
```

### Access Points

- **HDFS Web UI**: <http://localhost:9870>
- **Resource Manager**: <http://localhost:8088>
- **Hive Server**: <http://localhost:10002>
- **MySQL Source DB**: localhost:3306 (user: root, password: hadoop)
- **Spark UI**: <http://localhost:4040>

## Minimum Deliverables

1. ✅ Complete Hadoop ecosystem (HDFS, Hive, Pig, Sqoop)
2. ✅ MySQL source database with realistic business data
3. ✅ Multi-domain data generation (transactions, customers, products, logs)
4. ✅ Sqoop jobs for database-to-HDFS data transfer
5. ✅ Hive data warehouse with partitioned tables
6. ✅ Pig ETL scripts for data transformation
7. ✅ MapReduce jobs for complex analytics
8. ✅ Comprehensive analytics dashboard queries
9. ✅ Data quality validation and monitoring
10. ✅ Performance optimization and tuning

## Future Expansion Directions with Curated Resources

### 1. Real-time Stream Processing with Apache Kafka

**Learning Resources:**
- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/): Comprehensive Kafka implementation guide
- [Building Event-Driven Architectures](https://www.confluent.io/blog/event-driven-architecture-best-practices/): Event-driven design patterns and best practices
- [Kafka Streams in Action](https://www.manning.com/books/kafka-streams-in-action): Stream processing with Kafka Streams
- [Apache Kafka Performance Tuning](https://kafka.apache.org/documentation/#performance): Official performance optimization guide

**Implementation Projects:**
- Integrate Kafka for real-time transaction ingestion and processing
- Build Kafka Streams applications for real-time analytics and alerts
- Implement exactly-once semantics for critical business operations
- Create event sourcing patterns for data lineage and auditability

### 2. Advanced Analytics with Apache Spark MLlib

**Machine Learning Resources:**
- [Spark MLlib Programming Guide](https://spark.apache.org/docs/latest/ml-guide.html): Official MLlib documentation and examples
- [Learning Spark: Lightning-Fast Data Analytics](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/): Comprehensive Spark guide
- [Machine Learning Design Patterns](https://www.oreilly.com/library/view/machine-learning-design/9781098115777/): ML engineering best practices
- [Feature Engineering for Machine Learning](https://www.oreilly.com/library/view/feature-engineering-for/9781491953235/): Feature engineering techniques

**ML Enhancement Projects:**
- Build customer segmentation models using clustering algorithms
- Implement recommendation systems for product suggestions
- Create fraud detection models for transaction monitoring
- Develop time series forecasting for inventory management

### 3. Data Governance with Apache Atlas

**Data Governance Resources:**
- [Apache Atlas Architecture Guide](https://atlas.apache.org/#/Architecture): Atlas system architecture and components
- [Data Governance Best Practices](https://www.talend.com/resources/data-governance-best-practices/): Industry best practices guide
- [Data Lineage and Impact Analysis](https://www.informatica.com/resources/articles/what-is-data-lineage.html): Understanding data lineage concepts
- [Apache Ranger Security Guide](https://ranger.apache.org/): Fine-grained access control implementation

**Governance Implementation Projects:**
- Implement comprehensive data lineage tracking across all pipeline stages
- Build automated data quality monitoring and alerting systems
- Create fine-grained access control policies for sensitive data
- Develop data cataloging and discovery interfaces for business users

### 4. Advanced Workflow Orchestration with Apache Airflow

**Workflow Orchestration Resources:**
- [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow): Comprehensive Airflow guide
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html): Official best practices documentation
- [Building Robust Data Pipelines](https://www.oreilly.com/library/view/building-robust-data/9781492053187/): Pipeline reliability and monitoring
- [Dynamic Task Generation in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dynamic-task-mapping.html): Advanced DAG patterns

**Airflow Enhancement Projects:**
- Implement complex dependency management across multiple data sources
- Build dynamic DAG generation based on metadata configuration
- Create comprehensive pipeline monitoring and alerting systems
- Develop automated pipeline testing and validation frameworks

### 5. Cloud Platform Integration and Hybrid Architecture

**Cloud Integration Resources:**
- [AWS Big Data Analytics Options](https://aws.amazon.com/big-data/datalakes-and-analytics/): AWS analytics services overview
- [Azure HDInsight Documentation](https://docs.microsoft.com/en-us/azure/hdinsight/): Azure managed Hadoop services
- [Google Cloud Dataproc Guide](https://cloud.google.com/dataproc/docs): Google Cloud managed Spark and Hadoop
- [Multi-Cloud Data Strategy](https://www.mckinsey.com/business-functions/mckinsey-digital/our-insights/designing-data-governance-that-delivers-value): Multi-cloud considerations

**Cloud Migration Projects:**
- Implement hybrid cloud data processing with on-premises and cloud resources
- Build automated cloud resource provisioning based on workload demands
- Create cost optimization strategies for cloud-based data processing
- Develop disaster recovery and backup strategies across cloud providers

### 6. Advanced Security and Compliance

**Security Resources:**
- [Hadoop Security Guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html): Comprehensive Hadoop security implementation
- [Kerberos Authentication Deep Dive](https://web.mit.edu/kerberos/krb5-latest/doc/): MIT Kerberos documentation
- [Data Privacy Regulations Compliance](https://gdpr.eu/): GDPR and data privacy requirements
- [Zero Trust Data Architecture](https://www.nist.gov/publications/zero-trust-architecture): NIST zero trust guidelines

**Security Implementation Projects:**
- Implement end-to-end encryption for data at rest and in transit
- Build comprehensive audit logging and compliance reporting systems
- Create role-based access control with attribute-based policies
- Develop data anonymization and pseudonymization capabilities

### 7. Performance Optimization and Cost Management

**Performance Resources:**
- [Hadoop Performance Tuning Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html): Official performance optimization
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html): Spark optimization techniques
- [Hive Query Optimization](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Optimization): Hive performance best practices
- [Big Data Cost Optimization](https://aws.amazon.com/blogs/big-data/): Cost optimization strategies and techniques

**Optimization Projects:**
- Implement intelligent data partitioning strategies for optimal query performance
- Build automated performance monitoring and tuning recommendations
- Create cost-aware resource allocation and scheduling algorithms
- Develop workload-based capacity planning and scaling mechanisms

### 8. Data Visualization and Business Intelligence

**Visualization Resources:**
- [Apache Superset Documentation](https://superset.apache.org/): Open-source data visualization platform
- [Tableau Integration with Hadoop](https://www.tableau.com/solutions/hadoop): Tableau-Hadoop connectivity patterns
- [Power BI Big Data Integration](https://docs.microsoft.com/en-us/power-bi/connect-data/): Power BI data source connections
- [Data Storytelling Best Practices](https://www.storytellingwithdata.com/): Effective data visualization principles

**BI Enhancement Projects:**
- Build self-service analytics interfaces for business users
- Implement real-time dashboards for operational monitoring
- Create automated report generation and distribution systems
- Develop interactive data exploration tools with drill-down capabilities

### 9. Data Quality and Monitoring Excellence

**Data Quality Resources:**
- [Apache Griffin Data Quality](https://griffin.apache.org/): Open-source data quality platform
- [Great Expectations Framework](https://greatexpectations.io/): Data validation and documentation
- [Data Quality Patterns](https://www.oreilly.com/library/view/bad-data-handbook/9781449324957/): Common data quality issues and solutions
- [Observability for Data Pipelines](https://www.monte-carlo.ai/blog/data-observability/): Modern data observability practices

**Quality Enhancement Projects:**
- Implement comprehensive data profiling and quality scoring systems
- Build automated data anomaly detection and alerting mechanisms
- Create data validation frameworks with business rule engines
- Develop data lineage tracking for impact analysis and debugging

### 10. Advanced Integration Patterns and API Development

**Integration Resources:**
- [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/): Messaging and integration patterns
- [RESTful API Design Best Practices](https://restfulapi.net/): API design principles and patterns
- [Apache Camel Integration Guide](https://camel.apache.org/manual/latest/): Enterprise integration framework
- [Event-Driven Architecture Patterns](https://microservices.io/patterns/data/event-driven-architecture.html): Event-driven design patterns

**Integration Projects:**
- Build comprehensive REST APIs for data access and pipeline management
- Implement event-driven integration patterns for real-time data synchronization
- Create data federation layers for unified access across multiple systems
- Develop standard data exchange formats and protocols for ecosystem integration

## Architecture Diagram

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Hadoop Ecosystem Pipeline                    │
├─────────────────────────────────────────────────────────────────┤
│  Data Sources          │  Ingestion Layer    │  Storage Layer   │
│  ├─ MySQL Database     │  ├─ Sqoop Jobs      │  ├─ HDFS Raw     │
│  ├─ Log Files         │  ├─ File Uploads    │  ├─ HDFS Staged  │
│  ├─ API Feeds         │  └─ Stream Ingestion │  └─ HDFS Curated │
├─────────────────────────────────────────────────────────────────┤
│  Processing Layer      │  Analytics Layer    │  Access Layer    │
│  ├─ Pig ETL Scripts   │  ├─ Hive Warehouse  │  ├─ HiveQL       │
│  ├─ MapReduce Jobs    │  ├─ Spark SQL       │  ├─ Pig Latin    │
│  ├─ Spark Jobs        │  ├─ Complex Views   │  ├─ JDBC/ODBC    │
│  └─ Data Validation   │  └─ Analytics       │  └─ REST APIs    │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow Process

1. **Data Generation**: Create realistic multi-domain datasets
2. **Source Loading**: Populate MySQL with transactional data
3. **Data Ingestion**: Use Sqoop to transfer data to HDFS
4. **Raw Storage**: Store incoming data in HDFS raw zone
5. **ETL Processing**: Transform data using Pig and MapReduce
6. **Data Warehousing**: Load processed data into Hive tables
7. **Analytics**: Run complex queries and generate reports
8. **Quality Assurance**: Validate data integrity and completeness

## Learning Outcomes
- HDFS architecture and replication concepts
- MapReduce programming paradigm
- Docker containerization for big data
- Data pipeline design patterns
- Hadoop ecosystem overview
