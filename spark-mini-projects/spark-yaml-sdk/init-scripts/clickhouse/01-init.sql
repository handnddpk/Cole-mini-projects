-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- Switch to analytics database
USE analytics;

-- Create customer_analysis table
CREATE TABLE IF NOT EXISTS customer_analysis (
    customer_id UInt32,
    customer_name String,
    total_spent Float64
) ENGINE = MergeTree()
ORDER BY (customer_id);