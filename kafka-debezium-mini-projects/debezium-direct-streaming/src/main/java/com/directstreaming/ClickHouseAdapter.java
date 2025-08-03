package com.directstreaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ClickHouse Adapter for Direct Streaming
 * 
 * This adapter handles streaming data directly to ClickHouse using native protocols
 * for maximum performance. It implements batching, connection pooling, and error
 * handling optimized for real-time CDC workloads.
 */
@Component
public class ClickHouseAdapter {
    
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseAdapter.class);
    
    @Value("${clickhouse.host:localhost}")
    private String clickHouseHost;
    
    @Value("${clickhouse.port:8123}")
    private int clickHousePort;
    
    @Value("${clickhouse.database:default}")
    private String clickHouseDatabase;
    
    @Value("${clickhouse.user:default}")
    private String clickHouseUser;
    
    @Value("${clickhouse.password:}")
    private String clickHousePassword;
    
    @Value("${clickhouse.batch-size:1000}")
    private int batchSize;
    
    @Value("${clickhouse.flush-interval-ms:100}")
    private long flushIntervalMs;
    
    private Connection connection;
    private final ConcurrentLinkedQueue<TransformedRecord> recordQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final ScheduledExecutorService flushScheduler = Executors.newSingleThreadScheduledExecutor();
    
    @PostConstruct
    public void initialize() {
        logger.info("Initializing ClickHouse Adapter...");
        
        try {
            // Establish ClickHouse connection
            establishConnection();
            
            // Create target tables
            createTargetTables();
            
            // Start periodic flush scheduler
            startFlushScheduler();
            
            logger.info("ClickHouse Adapter initialized successfully");
            
        } catch (Exception e) {
            logger.error("Failed to initialize ClickHouse Adapter", e);
            throw new RuntimeException("ClickHouse initialization failed", e);
        }
    }
    
    /**
     * Stream a record to ClickHouse
     * Records are queued and batched for optimal performance
     */
    public void streamRecord(TransformedRecord record) {
        try {
            recordQueue.offer(record);
            int currentSize = queueSize.incrementAndGet();
            
            // Trigger immediate flush if batch size reached
            if (currentSize >= batchSize) {
                flushBatch();
            }
            
        } catch (Exception e) {
            logger.error("Failed to stream record to ClickHouse", e);
            throw new RuntimeException("Record streaming failed", e);
        }
    }
    
    /**
     * Establish connection to ClickHouse
     */
    private void establishConnection() throws SQLException {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", 
            clickHouseHost, clickHousePort, clickHouseDatabase);
            
        logger.info("Connecting to ClickHouse: {}", url);
        
        connection = DriverManager.getConnection(url, clickHouseUser, clickHousePassword);
        connection.setAutoCommit(false); // Use manual commits for batching
        
        logger.info("Connected to ClickHouse successfully");
    }
    
    /**
     * Create target tables in ClickHouse
     */
    private void createTargetTables() throws SQLException {
        createOrdersTable();
        createProductsTable();
        createCustomersTable();
    }
    
    private void createOrdersTable() throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS orders (
                id UInt64,
                customer_id UInt64,
                product_id UInt64,
                quantity UInt32,
                price Decimal64(2),
                total_amount Decimal64(2),
                status LowCardinality(String),
                created_at DateTime,
                updated_at DateTime,
                processed_at DateTime,
                _operation LowCardinality(String),
                _timestamp DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(_timestamp)
            PARTITION BY toYYYYMM(created_at)
            ORDER BY (customer_id, id)
            """;
            
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
            logger.info("Created/verified orders table");
        }
    }
    
    private void createProductsTable() throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS products (
                id UInt64,
                name String,
                description String,
                price Decimal64(2),
                category LowCardinality(String),
                stock_quantity UInt32,
                created_at DateTime,
                updated_at DateTime,
                processed_at DateTime,
                _operation LowCardinality(String),
                _timestamp DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(_timestamp)
            PARTITION BY toYYYYMM(created_at)
            ORDER BY (category, id)
            """;
            
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
            logger.info("Created/verified products table");
        }
    }
    
    private void createCustomersTable() throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS customers (
                id UInt64,
                first_name String,
                last_name String,
                email_hash String,
                phone String,
                address String,
                city String,
                state String,
                zip_code String,
                created_at DateTime,
                updated_at DateTime,
                processed_at DateTime,
                _operation LowCardinality(String),
                _timestamp DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(_timestamp)
            PARTITION BY toYYYYMM(created_at)
            ORDER BY (state, id)
            """;
            
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
            logger.info("Created/verified customers table");
        }
    }
    
    /**
     * Start the periodic flush scheduler
     */
    private void startFlushScheduler() {
        flushScheduler.scheduleAtFixedRate(
            this::flushBatch, 
            flushIntervalMs, 
            flushIntervalMs, 
            TimeUnit.MILLISECONDS
        );
        
        logger.info("Started flush scheduler with interval: {}ms", flushIntervalMs);
    }
    
    /**
     * Flush batched records to ClickHouse
     */
    private synchronized void flushBatch() {
        if (recordQueue.isEmpty()) {
            return;
        }
        
        int batchCount = 0;
        long startTime = System.nanoTime();
        
        try {
            // Process records by table
            Map<String, List<TransformedRecord>> recordsByTable = groupRecordsByTable();
            
            for (Map.Entry<String, List<TransformedRecord>> entry : recordsByTable.entrySet()) {
                String tableName = entry.getKey();
                List<TransformedRecord> records = entry.getValue();
                
                batchCount += processBatchForTable(tableName, records);
            }
            
            // Commit the batch
            connection.commit();
            
            long endTime = System.nanoTime();
            long latencyMicros = (endTime - startTime) / 1000;
            
            logger.debug("Flushed batch: {} records in {}μs", batchCount, latencyMicros);
            
        } catch (Exception e) {
            logger.error("Failed to flush batch to ClickHouse", e);
            
            try {
                connection.rollback();
            } catch (SQLException rollbackException) {
                logger.error("Failed to rollback batch", rollbackException);
            }
            
            throw new RuntimeException("Batch flush failed", e);
        }
    }
    
    /**
     * Group records by table name for efficient batching
     */
    private Map<String, List<TransformedRecord>> groupRecordsByTable() {
        Map<String, List<TransformedRecord>> recordsByTable = new HashMap<>();
        
        TransformedRecord record;
        while ((record = recordQueue.poll()) != null) {
            queueSize.decrementAndGet();
            
            recordsByTable.computeIfAbsent(record.getTableName(), k -> new ArrayList<>())
                         .add(record);
        }
        
        return recordsByTable;
    }
    
    /**
     * Process a batch of records for a specific table
     */
    private int processBatchForTable(String tableName, List<TransformedRecord> records) throws SQLException {
        switch (tableName) {
            case "orders":
                return processOrdersBatch(records);
            case "products":
                return processProductsBatch(records);
            case "customers":
                return processCustomersBatch(records);
            default:
                logger.warn("Unknown table: {}, skipping {} records", tableName, records.size());
                return 0;
        }
    }
    
    /**
     * Process orders batch
     */
    private int processOrdersBatch(List<TransformedRecord> records) throws SQLException {
        String insertSQL = """
            INSERT INTO orders (
                id, customer_id, product_id, quantity, price, total_amount, 
                status, created_at, updated_at, processed_at, _operation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
            
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            for (TransformedRecord record : records) {
                Map<String, Object> data = record.getAfterData();
                
                pstmt.setLong(1, ((Number) data.get("id")).longValue());
                pstmt.setLong(2, ((Number) data.get("customer_id")).longValue());
                pstmt.setLong(3, ((Number) data.get("product_id")).longValue());
                pstmt.setInt(4, ((Number) data.get("quantity")).intValue());
                pstmt.setBigDecimal(5, new BigDecimal(data.get("price").toString()));
                pstmt.setBigDecimal(6, new BigDecimal(data.getOrDefault("total_amount", "0").toString()));
                pstmt.setString(7, (String) data.get("status"));
                pstmt.setTimestamp(8, new Timestamp(((Number) data.get("created_at")).longValue()));
                pstmt.setTimestamp(9, new Timestamp(((Number) data.get("updated_at")).longValue()));
                pstmt.setTimestamp(10, new Timestamp(((Number) data.get("processed_at")).longValue()));
                pstmt.setString(11, record.getOperation().toString());
                
                pstmt.addBatch();
            }
            
            int[] results = pstmt.executeBatch();
            return results.length;
        }
    }
    
    /**
     * Process products batch
     */
    private int processProductsBatch(List<TransformedRecord> records) throws SQLException {
        String insertSQL = """
            INSERT INTO products (
                id, name, description, price, category, stock_quantity,
                created_at, updated_at, processed_at, _operation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
            
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            for (TransformedRecord record : records) {
                Map<String, Object> data = record.getAfterData();
                
                pstmt.setLong(1, ((Number) data.get("id")).longValue());
                pstmt.setString(2, (String) data.get("name"));
                pstmt.setString(3, (String) data.get("description"));
                pstmt.setBigDecimal(4, new BigDecimal(data.get("price").toString()));
                pstmt.setString(5, (String) data.get("category"));
                pstmt.setInt(6, ((Number) data.get("stock_quantity")).intValue());
                pstmt.setTimestamp(7, new Timestamp(((Number) data.get("created_at")).longValue()));
                pstmt.setTimestamp(8, new Timestamp(((Number) data.get("updated_at")).longValue()));
                pstmt.setTimestamp(9, new Timestamp(((Number) data.get("processed_at")).longValue()));
                pstmt.setString(10, record.getOperation().toString());
                
                pstmt.addBatch();
            }
            
            int[] results = pstmt.executeBatch();
            return results.length;
        }
    }
    
    /**
     * Process customers batch
     */
    private int processCustomersBatch(List<TransformedRecord> records) throws SQLException {
        String insertSQL = """
            INSERT INTO customers (
                id, first_name, last_name, email_hash, phone, address,
                city, state, zip_code, created_at, updated_at, processed_at, _operation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
            
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            for (TransformedRecord record : records) {
                Map<String, Object> data = record.getAfterData();
                
                pstmt.setLong(1, ((Number) data.get("id")).longValue());
                pstmt.setString(2, (String) data.get("first_name"));
                pstmt.setString(3, (String) data.get("last_name"));
                pstmt.setString(4, (String) data.get("email_hash"));
                pstmt.setString(5, (String) data.get("phone"));
                pstmt.setString(6, (String) data.get("address"));
                pstmt.setString(7, (String) data.get("city"));
                pstmt.setString(8, (String) data.get("state"));
                pstmt.setString(9, (String) data.get("zip_code"));
                pstmt.setTimestamp(10, new Timestamp(((Number) data.get("created_at")).longValue()));
                pstmt.setTimestamp(11, new Timestamp(((Number) data.get("updated_at")).longValue()));
                pstmt.setTimestamp(12, new Timestamp(((Number) data.get("processed_at")).longValue()));
                pstmt.setString(13, record.getOperation().toString());
                
                pstmt.addBatch();
            }
            
            int[] results = pstmt.executeBatch();
            return results.length;
        }
    }
    
    /**
     * Check if the adapter is healthy
     */
    public boolean isHealthy() {
        try {
            if (connection == null || connection.isClosed()) {
                return false;
            }
            
            // Simple health check query
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1");
                return rs.next();
            }
            
        } catch (SQLException e) {
            logger.error("Health check failed", e);
            return false;
        }
    }
    
    /**
     * Get current queue size
     */
    public int getQueueSize() {
        return queueSize.get();
    }
    
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down ClickHouse Adapter...");
        
        try {
            // Flush any remaining records
            flushBatch();
            
            // Shutdown scheduler
            flushScheduler.shutdown();
            if (!flushScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                flushScheduler.shutdownNow();
            }
            
            // Close connection
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
            
            logger.info("ClickHouse Adapter shutdown complete");
            
        } catch (Exception e) {
            logger.error("Error during ClickHouse Adapter shutdown", e);
        }
    }
}
