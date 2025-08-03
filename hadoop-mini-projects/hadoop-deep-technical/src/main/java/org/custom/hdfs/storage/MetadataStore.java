package org.custom.hdfs.storage;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * HDFS Metadata Management on MinIO - Core Interface
 * 
 * This interface defines the metadata store abstraction for simulating HDFS
 * metadata operations on top of MinIO object storage, demonstrating the
 * fundamental differences between traditional filesystem metadata and
 * object storage metadata patterns.
 * 
 * KEY LEARNING OBJECTIVES:
 * 1. Understand HDFS namespace tree representation in relational storage
 * 2. Learn about consistency challenges when mapping filesystem ops to object storage
 * 3. Implement caching strategies for metadata performance optimization
 * 4. Handle atomic operations (rename, delete) that require distributed transactions
 * 
 * IMPLEMENTATION REQUIREMENTS:
 * - Use PostgreSQL for ACID transaction support
 * - Implement Redis caching for hot metadata
 * - Handle MinIO eventual consistency model
 * - Support HDFS WebHDFS API semantics
 * 
 * PERFORMANCE CONSIDERATIONS:
 * - Directory listing: O(1) vs O(n) comparison with object storage prefixes
 * - Rename operations: Atomic vs copy+delete trade-offs
 * - Cache invalidation strategies for consistency
 * - Batch operations for reducing round-trip latency
 */
public interface MetadataStore {
    
    
    /**
     * Create a new directory entry in the namespace
     */
    void createDirectory(String path, FileMetadata metadata) throws IOException;
    
    /**
     * Create a new file entry with block mapping
     */
    void createFile(String path, FileMetadata metadata, List<BlockInfo> blocks) throws IOException;
    
    /**
     * Get file or directory metadata
     */
    Optional<FileMetadata> getMetadata(String path) throws IOException;
    
    /**
     * List directory contents (simulates HDFS listStatus)
     */
    List<FileMetadata> listDirectory(String path) throws IOException;
    
    /**
     * Delete file or directory
     * For directories: recursive delete with consistency guarantees
     */
    void delete(String path, boolean recursive) throws IOException;
    
    /**
     * Rename operation - atomic across metadata and object storage
     * This is the most complex operation due to object storage limitations
     */
    void rename(String sourcePath, String destPath) throws IOException;
    
    /**
     * Get block locations for a file
     */
    List<BlockInfo> getFileBlocks(String path) throws IOException;
    
    /**
     * Update block locations after MinIO operations
     */
    void updateBlockLocations(String path, List<BlockInfo> blocks) throws IOException;
    
    /**
     * Health check for the metadata service
     */
    boolean isHealthy();
    
    /**
     * Get performance metrics
     */
    MetadataStats getStats();
    
    /**
     * Close connections and cleanup resources
     */
    void close() throws IOException;
    
    /**
     * File and directory metadata structure
     */
    class FileMetadata {
        public final String path;
        public final String name;
        public final boolean isDirectory;
        public final long size;
        public final long blockSize;
        public final short replication;
        public final String owner;
        public final String group;
        public final String permissions;
        public final long createdTime;
        public final long modifiedTime;
        public final long accessedTime;
        
        public FileMetadata(String path, String name, boolean isDirectory, 
                          long size, long blockSize, short replication,
                          String owner, String group, String permissions,
                          long createdTime, long modifiedTime, long accessedTime) {
            this.path = path;
            this.name = name;
            this.isDirectory = isDirectory;
            this.size = size;
            this.blockSize = blockSize;
            this.replication = replication;
            this.owner = owner;
            this.group = group;
            this.permissions = permissions;
            this.createdTime = createdTime;
            this.modifiedTime = modifiedTime;
            this.accessedTime = accessedTime;
        }
    }
    
    /**
     * Block information mapping to MinIO objects
     */
    class BlockInfo {
        public final long blockId;
        public final String filePath;
        public final int blockIndex;
        public final long blockSize;
        public final String minioObjectKey;
        public final List<String> locations; // MinIO server nodes
        public final long generationStamp;
        
        public BlockInfo(long blockId, String filePath, int blockIndex, 
                        long blockSize, String minioObjectKey, 
                        List<String> locations, long generationStamp) {
            this.blockId = blockId;
            this.filePath = filePath;
            this.blockIndex = blockIndex;
            this.blockSize = blockSize;
            this.minioObjectKey = minioObjectKey;
            this.locations = locations;
            this.generationStamp = generationStamp;
        }
    }
    
    /**
     * Performance and operational statistics
     */
    class MetadataStats {
        public final long totalFiles;
        public final long totalDirectories;
        public final long totalBlocks;
        public final double avgOperationLatency;
        public final double cacheHitRatio;
        public final long postgresConnections;
        public final long redisConnections;
        
        public MetadataStats(long totalFiles, long totalDirectories, long totalBlocks,
                           double avgOperationLatency, double cacheHitRatio,
                           long postgresConnections, long redisConnections) {
            this.totalFiles = totalFiles;
            this.totalDirectories = totalDirectories;
            this.totalBlocks = totalBlocks;
            this.avgOperationLatency = avgOperationLatency;
            this.cacheHitRatio = cacheHitRatio;
            this.postgresConnections = postgresConnections;
            this.redisConnections = redisConnections;
        }
    }
}
