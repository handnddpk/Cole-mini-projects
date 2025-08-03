package com.directstreaming;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Direct Streaming Engine - Bypasses Kafka for CDC
 * 
 * This component uses Debezium's embedded engine to capture changes directly
 * from MySQL binlog and streams them to target systems without intermediate
 * Kafka topics, achieving ultra-low latency data replication.
 */
@Component
public class DirectStreamingEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(DirectStreamingEngine.class);
    
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private ExecutorService executor;
    
    @Autowired
    private ClickHouseAdapter clickHouseAdapter;
    
    @Autowired
    private StreamingMetrics metrics;
    
    @Autowired
    private StreamingConfiguration config;
    
    @PostConstruct
    public void start() {
        logger.info("Starting Direct Streaming Engine...");
        
        // Use virtual threads for maximum concurrency (Project Loom)
        executor = Executors.newVirtualThreadPerTaskExecutor();
        
        // Configure Debezium embedded engine
        Configuration debeziumConfig = Configuration.create()
            .with("name", "direct-streaming-engine")
            .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
            .with("database.hostname", config.getMysqlHost())
            .with("database.port", config.getMysqlPort())
            .with("database.user", config.getMysqlUser())
            .with("database.password", config.getMysqlPassword())
            .with("database.server.id", config.getServerId())
            .with("database.server.name", "mysql-direct")
            .with("table.include.list", config.getTableIncludeList())
            .with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory")
            .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .with("offset.flush.interval.ms", 1000)
            .with("snapshot.mode", "initial")
            .with("decimal.handling.mode", "double")
            .with("time.precision.mode", "adaptive")
            .build();
        
        // Create embedded engine with custom change event handler
        engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
            .using(debeziumConfig.asProperties())
            .notifying(this::handleChangeEvent)
            .using(this.getClass().getClassLoader())
            .build();
            
        // Start the engine in a separate thread
        executor.submit(engine);
        
        logger.info("Direct Streaming Engine started successfully");
    }
    
    /**
     * Handle change events from Debezium
     * This is where the magic happens - we bypass Kafka and stream directly
     */
    private void handleChangeEvent(RecordChangeEvent<SourceRecord> event) {
        try {
            SourceRecord record = event.record();
            
            // Start latency measurement
            long startTime = System.nanoTime();
            
            // Extract change information
            ChangeRecord changeRecord = extractChangeData(record);
            
            if (changeRecord != null) {
                // Apply any transformations
                TransformedRecord transformed = applyTransformations(changeRecord);
                
                // Stream directly to target system (non-blocking)
                executor.execute(() -> {
                    try {
                        clickHouseAdapter.streamRecord(transformed);
                        
                        // Record metrics
                        long endTime = System.nanoTime();
                        long latencyMicros = (endTime - startTime) / 1000;
                        metrics.recordProcessingLatency(latencyMicros);
                        metrics.incrementRecordsProcessed(transformed.getOperation());
                        
                        logger.debug("Streamed record: table={}, operation={}, latency={}μs", 
                            transformed.getTableName(), 
                            transformed.getOperation(), 
                            latencyMicros);
                            
                    } catch (Exception e) {
                        logger.error("Failed to stream record to ClickHouse", e);
                        metrics.incrementErrorCount(e.getClass().getSimpleName());
                        handleStreamingError(transformed, e);
                    }
                });
            }
            
        } catch (Exception e) {
            logger.error("Error processing change event", e);
            metrics.incrementErrorCount(e.getClass().getSimpleName());
        }
    }
    
    /**
     * Extract change data from Debezium SourceRecord
     */
    private ChangeRecord extractChangeData(SourceRecord record) {
        try {
            Struct value = (Struct) record.value();
            if (value == null) {
                return null; // Tombstone record
            }
            
            String operation = value.getString("op");
            Struct after = value.getStruct("after");
            Struct before = value.getStruct("before");
            Struct source = value.getStruct("source");
            
            String tableName = source.getString("table");
            String databaseName = source.getString("db");
            Long timestamp = source.getInt64("ts_ms");
            
            return ChangeRecord.builder()
                .operation(ChangeOperation.fromDebeziumOp(operation))
                .tableName(tableName)
                .databaseName(databaseName)
                .beforeData(structToMap(before))
                .afterData(structToMap(after))
                .timestamp(timestamp)
                .build();
                
        } catch (Exception e) {
            logger.error("Failed to extract change data from record", e);
            return null;
        }
    }
    
    /**
     * Apply transformations to change records
     * This is where you can add custom business logic
     */
    private TransformedRecord applyTransformations(ChangeRecord changeRecord) {
        // Example transformations:
        // 1. Data type conversions
        // 2. Field mappings
        // 3. Data enrichment
        // 4. Filtering
        
        TransformedRecord.Builder builder = TransformedRecord.builder()
            .operation(changeRecord.getOperation())
            .tableName(changeRecord.getTableName())
            .databaseName(changeRecord.getDatabaseName())
            .timestamp(changeRecord.getTimestamp());
            
        // Apply table-specific transformations
        switch (changeRecord.getTableName()) {
            case "orders":
                return transformOrderRecord(builder, changeRecord);
            case "products":
                return transformProductRecord(builder, changeRecord);
            case "customers":
                return transformCustomerRecord(builder, changeRecord);
            default:
                return builder
                    .beforeData(changeRecord.getBeforeData())
                    .afterData(changeRecord.getAfterData())
                    .build();
        }
    }
    
    private TransformedRecord transformOrderRecord(TransformedRecord.Builder builder, ChangeRecord record) {
        // Example: Add calculated fields, format data types, etc.
        Map<String, Object> transformedData = new HashMap<>(record.getAfterData());
        
        // Add calculated total
        if (transformedData.containsKey("quantity") && transformedData.containsKey("price")) {
            Double quantity = (Double) transformedData.get("quantity");
            Double price = (Double) transformedData.get("price");
            transformedData.put("total_amount", quantity * price);
        }
        
        // Add processing timestamp
        transformedData.put("processed_at", Instant.now().toEpochMilli());
        
        return builder
            .beforeData(record.getBeforeData())
            .afterData(transformedData)
            .build();
    }
    
    private TransformedRecord transformProductRecord(TransformedRecord.Builder builder, ChangeRecord record) {
        // Product-specific transformations
        return builder
            .beforeData(record.getBeforeData())
            .afterData(record.getAfterData())
            .build();
    }
    
    private TransformedRecord transformCustomerRecord(TransformedRecord.Builder builder, ChangeRecord record) {
        // Customer-specific transformations (e.g., PII masking)
        Map<String, Object> transformedData = new HashMap<>(record.getAfterData());
        
        // Example: Mask sensitive data
        if (transformedData.containsKey("email")) {
            String email = (String) transformedData.get("email");
            transformedData.put("email_hash", hashPII(email));
            // Remove original email for privacy
            transformedData.remove("email");
        }
        
        return builder
            .beforeData(record.getBeforeData())
            .afterData(transformedData)
            .build();
    }
    
    /**
     * Handle streaming errors with retry logic and dead letter queue
     */
    private void handleStreamingError(TransformedRecord record, Exception error) {
        // Implement retry logic, circuit breaker, and dead letter queue
        // This is crucial for production resilience
        
        logger.error("Streaming error for record: table={}, operation={}, error={}", 
            record.getTableName(), 
            record.getOperation(), 
            error.getMessage());
            
        // Add to retry queue or dead letter queue based on error type
        if (isRetryableError(error)) {
            // Add to retry queue
            retryQueue.offer(record);
        } else {
            // Send to dead letter queue
            deadLetterQueue.offer(record);
        }
    }
    
    private boolean isRetryableError(Exception error) {
        // Determine if error is retryable (connection issues, timeouts)
        // vs non-retryable (data format issues, schema mismatches)
        return error instanceof ConnectException || 
               error instanceof TimeoutException ||
               error instanceof SQLTransientException;
    }
    
    private String hashPII(String data) {
        // Simple hash for PII protection (use proper encryption in production)
        return DigestUtils.sha256Hex(data);
    }
    
    private Map<String, Object> structToMap(Struct struct) {
        if (struct == null) return null;
        
        Map<String, Object> map = new HashMap<>();
        struct.schema().fields().forEach(field -> {
            map.put(field.name(), struct.get(field));
        });
        return map;
    }
    
    @PreDestroy
    public void stop() {
        logger.info("Stopping Direct Streaming Engine...");
        
        try {
            if (engine != null) {
                engine.close();
            }
            
            if (executor != null) {
                executor.shutdown();
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            }
            
            logger.info("Direct Streaming Engine stopped successfully");
            
        } catch (Exception e) {
            logger.error("Error stopping Direct Streaming Engine", e);
        }
    }
}
