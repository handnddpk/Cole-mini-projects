#!/bin/bash
"""
Comprehensive Hadoop Ecosystem Pipeline
Integrates HDFS, Hive, Pig, Sqoop, and MapReduce for complete data processing
"""

set -e

echo "=== Hadoop Ecosystem Comprehensive Pipeline ==="
echo "Starting complete data processing pipeline..."

# Configuration
HDFS_URI="hdfs://namenode:8020"
MYSQL_HOST="mysql-source"
MYSQL_DB="retail_db"
MYSQL_USER="sqoop"
MYSQL_PASSWORD="sqoop"

# Step 1: Prepare Hive data warehouse
echo "Step 1: Setting up Hive data warehouse..."

# Create Hive database and tables
hive -e "
CREATE DATABASE IF NOT EXISTS retail_dw
COMMENT 'Retail Data Warehouse'
LOCATION '/warehouse/retail_dw';

USE retail_dw;

-- Create external table for raw customer data
CREATE EXTERNAL TABLE IF NOT EXISTS customers_raw (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    age INT,
    gender STRING,
    city STRING,
    state STRING,
    country STRING,
    registration_date DATE,
    loyalty_tier STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/warehouse/customers_raw/';

-- Create external table for raw product data
CREATE EXTERNAL TABLE IF NOT EXISTS products_raw (
    product_id INT,
    name STRING,
    category STRING,
    subcategory STRING,
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    supplier_id INT,
    stock_quantity INT,
    created_date DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/warehouse/products_raw/';

-- Create external table for raw transaction data
CREATE EXTERNAL TABLE IF NOT EXISTS transactions_raw (
    transaction_id STRING,
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method STRING,
    transaction_date TIMESTAMP,
    city STRING,
    state STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/warehouse/transactions_raw/';

-- Create partitioned table for processed transactions
CREATE TABLE IF NOT EXISTS transactions_processed (
    transaction_id STRING,
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method STRING,
    transaction_timestamp TIMESTAMP,
    city STRING,
    state STRING,
    year INT,
    month INT
)
PARTITIONED BY (transaction_date STRING)
STORED AS PARQUET
LOCATION '/warehouse/transactions_processed/';

-- Create aggregated sales table
CREATE TABLE IF NOT EXISTS daily_sales_summary (
    sale_date DATE,
    category STRING,
    total_transactions INT,
    total_revenue DECIMAL(15,2),
    avg_transaction_value DECIMAL(10,2),
    unique_customers INT
)
STORED AS PARQUET
LOCATION '/warehouse/daily_sales_summary/';
"

echo "Hive data warehouse setup completed"

# Step 2: Use Sqoop to import data from MySQL to HDFS
echo "Step 2: Importing data from MySQL using Sqoop..."

# Create HDFS directories
hdfs dfs -mkdir -p /warehouse/customers_raw
hdfs dfs -mkdir -p /warehouse/products_raw
hdfs dfs -mkdir -p /warehouse/transactions_raw
hdfs dfs -mkdir -p /warehouse/suppliers_raw

# Import customers data
sqoop import \
    --connect jdbc:mysql://${MYSQL_HOST}:3306/${MYSQL_DB} \
    --username ${MYSQL_USER} \
    --password ${MYSQL_PASSWORD} \
    --table customers \
    --target-dir /warehouse/customers_raw \
    --fields-terminated-by ',' \
    --lines-terminated-by '\n' \
    --num-mappers 4 \
    --delete-target-dir

# Import products data
sqoop import \
    --connect jdbc:mysql://${MYSQL_HOST}:3306/${MYSQL_DB} \
    --username ${MYSQL_USER} \
    --password ${MYSQL_PASSWORD} \
    --table products \
    --target-dir /warehouse/products_raw \
    --fields-terminated-by ',' \
    --lines-terminated-by '\n' \
    --num-mappers 4 \
    --delete-target-dir

# Import transactions data with incremental load capability
sqoop import \
    --connect jdbc:mysql://${MYSQL_HOST}:3306/${MYSQL_DB} \
    --username ${MYSQL_USER} \
    --password ${MYSQL_PASSWORD} \
    --table transactions \
    --target-dir /warehouse/transactions_raw \
    --fields-terminated-by ',' \
    --lines-terminated-by '\n' \
    --num-mappers 8 \
    --delete-target-dir

# Import suppliers data
sqoop import \
    --connect jdbc:mysql://${MYSQL_HOST}:3306/${MYSQL_DB} \
    --username ${MYSQL_USER} \
    --password ${MYSQL_PASSWORD} \
    --table suppliers \
    --target-dir /warehouse/suppliers_raw \
    --fields-terminated-by ',' \
    --lines-terminated-by '\n' \
    --num-mappers 2 \
    --delete-target-dir

echo "Sqoop data import completed"

# Step 3: Use Pig for ETL processing
echo "Step 3: Processing data using Pig Latin..."

# Create Pig ETL script
cat > /tmp/etl_processing.pig << 'EOF'
-- Load raw transaction data
raw_transactions = LOAD '/warehouse/transactions_raw' USING PigStorage(',') AS (
    transaction_id:chararray,
    customer_id:int,
    product_id:int,
    quantity:int,
    unit_price:double,
    total_amount:double,
    payment_method:chararray,
    transaction_date:chararray,
    city:chararray,
    state:chararray
);

-- Load customer data
customers = LOAD '/warehouse/customers_raw' USING PigStorage(',') AS (
    customer_id:int,
    first_name:chararray,
    last_name:chararray,
    email:chararray,
    phone:chararray,
    age:int,
    gender:chararray,
    city:chararray,
    state:chararray,
    country:chararray,
    registration_date:chararray,
    loyalty_tier:chararray
);

-- Load product data
products = LOAD '/warehouse/products_raw' USING PigStorage(',') AS (
    product_id:int,
    name:chararray,
    category:chararray,
    subcategory:chararray,
    price:double,
    cost:double,
    supplier_id:int,
    stock_quantity:int,
    created_date:chararray
);

-- Clean and transform transaction data
cleaned_transactions = FOREACH raw_transactions GENERATE
    transaction_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    ToDate(transaction_date, 'yyyy-MM-dd HH:mm:ss') AS transaction_timestamp,
    city,
    state,
    GetYear(ToDate(transaction_date, 'yyyy-MM-dd HH:mm:ss')) AS year,
    GetMonth(ToDate(transaction_date, 'yyyy-MM-dd HH:mm:ss')) AS month,
    ToString(ToDate(transaction_date, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS transaction_date;

-- Join transactions with products for enrichment
enriched_transactions = JOIN cleaned_transactions BY product_id, products BY product_id;

-- Project enriched data
final_transactions = FOREACH enriched_transactions GENERATE
    cleaned_transactions::transaction_id AS transaction_id,
    cleaned_transactions::customer_id AS customer_id,
    cleaned_transactions::product_id AS product_id,
    products::name AS product_name,
    products::category AS category,
    products::subcategory AS subcategory,
    cleaned_transactions::quantity AS quantity,
    cleaned_transactions::unit_price AS unit_price,
    cleaned_transactions::total_amount AS total_amount,
    cleaned_transactions::payment_method AS payment_method,
    cleaned_transactions::transaction_timestamp AS transaction_timestamp,
    cleaned_transactions::city AS city,
    cleaned_transactions::state AS state,
    cleaned_transactions::year AS year,
    cleaned_transactions::month AS month,
    cleaned_transactions::transaction_date AS transaction_date;

-- Store processed transactions
STORE final_transactions INTO '/warehouse/transactions_processed_pig' USING PigStorage(',');

-- Generate daily sales summary
grouped_by_date_category = GROUP final_transactions BY (transaction_date, category);

daily_summary = FOREACH grouped_by_date_category GENERATE
    FLATTEN(group) AS (sale_date, category),
    COUNT(final_transactions) AS total_transactions,
    SUM(final_transactions.total_amount) AS total_revenue,
    AVG(final_transactions.total_amount) AS avg_transaction_value;

-- Store daily summary
STORE daily_summary INTO '/warehouse/daily_sales_pig' USING PigStorage(',');

-- Customer analysis
customer_transactions = JOIN cleaned_transactions BY customer_id, customers BY customer_id;

customer_summary = GROUP customer_transactions BY (customers::customer_id, customers::loyalty_tier, customers::age);

customer_analysis = FOREACH customer_summary GENERATE
    FLATTEN(group) AS (customer_id, loyalty_tier, age),
    COUNT(customer_transactions) AS total_transactions,
    SUM(customer_transactions.total_amount) AS total_spent,
    AVG(customer_transactions.total_amount) AS avg_transaction_value;

-- Store customer analysis
STORE customer_analysis INTO '/warehouse/customer_analysis_pig' USING PigStorage(',');
EOF

# Run Pig script
pig -f /tmp/etl_processing.pig

echo "Pig ETL processing completed"

# Step 4: Load processed data into Hive tables
echo "Step 4: Loading processed data into Hive tables..."

hive -e "
USE retail_dw;

-- Load processed transactions into Hive table
LOAD DATA INPATH '/warehouse/transactions_processed_pig/part*' 
INTO TABLE transactions_processed 
PARTITION (transaction_date='2024-01-01');

-- Load daily sales summary
INSERT OVERWRITE TABLE daily_sales_summary
SELECT 
    CAST(sale_date AS DATE),
    category,
    CAST(total_transactions AS INT),
    CAST(total_revenue AS DECIMAL(15,2)),
    CAST(avg_transaction_value AS DECIMAL(10,2)),
    0 AS unique_customers
FROM (
    SELECT * FROM daily_sales_summary
    UNION ALL
    SELECT 
        sale_date,
        category,
        total_transactions,
        total_revenue,
        avg_transaction_value
    FROM EXTERNAL_TABLE('/warehouse/daily_sales_pig')
) combined_data;
"

echo "Data loaded into Hive tables"

# Step 5: Run advanced analytics with HiveQL
echo "Step 5: Running advanced analytics with HiveQL..."

hive -e "
USE retail_dw;

-- Top 10 selling products by revenue
SELECT 
    p.name AS product_name,
    p.category,
    SUM(t.total_amount) AS total_revenue,
    COUNT(*) AS total_transactions,
    AVG(t.total_amount) AS avg_transaction_value
FROM transactions_raw t
JOIN products_raw p ON t.product_id = p.product_id
GROUP BY p.product_id, p.name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- Customer segmentation by loyalty tier and spending
SELECT 
    c.loyalty_tier,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    AVG(customer_metrics.total_spent) AS avg_customer_lifetime_value,
    AVG(customer_metrics.transaction_count) AS avg_transactions_per_customer
FROM customers_raw c
JOIN (
    SELECT 
        customer_id,
        SUM(total_amount) AS total_spent,
        COUNT(*) AS transaction_count
    FROM transactions_raw
    GROUP BY customer_id
) customer_metrics ON c.customer_id = customer_metrics.customer_id
GROUP BY c.loyalty_tier
ORDER BY avg_customer_lifetime_value DESC;

-- Monthly revenue trend by category
SELECT 
    YEAR(transaction_date) AS year,
    MONTH(transaction_date) AS month,
    p.category,
    SUM(t.total_amount) AS monthly_revenue,
    COUNT(*) AS transaction_count
FROM transactions_raw t
JOIN products_raw p ON t.product_id = p.product_id
GROUP BY YEAR(transaction_date), MONTH(transaction_date), p.category
ORDER BY year DESC, month DESC, monthly_revenue DESC;
" > /data/analytics_results.txt

echo "Advanced analytics completed"

# Step 6: Create MapReduce job for complex analytics
echo "Step 6: Running custom MapReduce job..."

# Create advanced MapReduce job
mkdir -p /tmp/mapreduce-advanced
cd /tmp/mapreduce-advanced

cat > CustomerAnalysis.java << 'EOF'
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerAnalysis {

    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            if (line.startsWith("transaction_id")) return; // Skip header
            
            try {
                String[] fields = line.split(",");
                if (fields.length >= 10) {
                    String customerId = fields[1].trim();
                    String totalAmount = fields[5].trim();
                    String paymentMethod = fields[6].trim();
                    String city = fields[8].trim();
                    String state = fields[9].trim();
                    
                    // Create analysis record
                    String analysisData = totalAmount + "," + paymentMethod + "," + city + "," + state;
                    context.write(new Text(customerId), new Text(analysisData));
                }
            } catch (Exception e) {
                System.err.println("Error processing line: " + line);
            }
        }
    }
    
    public static class CustomerReducer extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            double totalSpent = 0.0;
            int transactionCount = 0;
            Map<String, Integer> paymentMethods = new HashMap<>();
            Map<String, Integer> cities = new HashMap<>();
            
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length >= 4) {
                    // Sum total amount
                    totalSpent += Double.parseDouble(parts[0]);
                    transactionCount++;
                    
                    // Count payment methods
                    String paymentMethod = parts[1];
                    paymentMethods.put(paymentMethod, paymentMethods.getOrDefault(paymentMethod, 0) + 1);
                    
                    // Count cities
                    String city = parts[2];
                    cities.put(city, cities.getOrDefault(city, 0) + 1);
                }
            }
            
            // Find most frequent payment method and city
            String mostFrequentPayment = paymentMethods.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("Unknown");
                
            String mostFrequentCity = cities.entrySet().stream()  
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("Unknown");
            
            double avgTransactionValue = totalSpent / transactionCount;
            
            String result = String.format("%.2f,%d,%.2f,%s,%s", 
                totalSpent, transactionCount, avgTransactionValue, 
                mostFrequentPayment, mostFrequentCity);
            
            context.write(key, new Text(result));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "customer analysis");
        
        job.setJarByClass(CustomerAnalysis.class);
        job.setMapperClass(CustomerMapper.class);
        job.setReducerClass(CustomerReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
EOF

# Compile and run MapReduce job
javac -classpath $(hadoop classpath) CustomerAnalysis.java
jar cf customer-analysis.jar CustomerAnalysis*.class

# Run the job
hdfs dfs -rm -r -f /output/customer-analysis
hadoop jar customer-analysis.jar CustomerAnalysis /warehouse/transactions_raw /output/customer-analysis

echo "MapReduce customer analysis completed"

# Step 7: Generate comprehensive report
echo "Step 7: Generating comprehensive pipeline report..."

cat > /data/pipeline_report.md << EOF
# Hadoop Ecosystem Pipeline Report

**Execution Date**: $(date)
**Pipeline Duration**: $SECONDS seconds

## Data Processing Summary

### 1. Data Import (Sqoop)
- **Customers**: $(hdfs dfs -count /warehouse/customers_raw | awk '{print $2}') files
- **Products**: $(hdfs dfs -count /warehouse/products_raw | awk '{print $2}') files  
- **Transactions**: $(hdfs dfs -count /warehouse/transactions_raw | awk '{print $2}') files
- **Suppliers**: $(hdfs dfs -count /warehouse/suppliers_raw | awk '{print $2}') files

### 2. ETL Processing (Pig)
- **Processed Transactions**: $(hdfs dfs -count /warehouse/transactions_processed_pig | awk '{print $2}') files
- **Daily Sales Summary**: $(hdfs dfs -count /warehouse/daily_sales_pig | awk '{print $2}') files
- **Customer Analysis**: $(hdfs dfs -count /warehouse/customer_analysis_pig | awk '{print $2}') files

### 3. Data Warehouse (Hive)
- **Database**: retail_dw
- **Tables Created**: 5 tables (customers_raw, products_raw, transactions_raw, transactions_processed, daily_sales_summary)
- **Analytics Queries**: Executed successfully

### 4. Advanced Analytics (MapReduce)
- **Customer Analysis Job**: Completed
- **Output Location**: /output/customer-analysis

## Storage Usage
$(hdfs dfs -du -s -h /warehouse/)

## Access Information
- **Hive Server**: hive-server:10000
- **HDFS Web UI**: http://namenode:9870
- **Results Location**: /data/analytics_results.txt

## Next Steps
1. Set up automated data ingestion with Sqoop scheduled jobs
2. Create more sophisticated Pig ETL workflows
3. Build Hive views for business users
4. Implement data quality monitoring
5. Add real-time processing with Kafka and Spark Streaming

EOF

echo ""
echo "=== Pipeline Execution Complete ==="
echo "Complete report available at: /data/pipeline_report.md"
echo "Analytics results: /data/analytics_results.txt"
echo "Access Hive: beeline -u jdbc:hive2://hive-server:10000"
echo "Access HDFS: http://namenode:9870"
