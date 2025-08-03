package org.custom.hdfs.metadata;

import org.custom.hdfs.storage.MetadataStore;
import org.custom.hdfs.storage.MetadataStore.FileMetadata;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Namespace Manager for HDFS Metadata on MinIO
 * 
 * This class manages the hierarchical namespace structure that HDFS provides,
 * mapping it to PostgreSQL relational storage while maintaining the filesystem
 * semantics that HDFS clients expect.
 * 
 * KEY RESPONSIBILITIES:
 * 1. Maintain namespace tree integrity and consistency
 * 2. Handle path resolution and validation
 * 3. Coordinate metadata operations with object storage
 * 4. Implement efficient caching for hot paths
 * 
 * DESIGN CHALLENGES:
 * - Path normalization and validation (e.g., "../", "//", trailing slashes)
 * - Concurrent access to namespace tree with proper locking
 * - Parent-child relationship integrity during renames and deletes
 * - Efficient directory listing with pagination support
 * 
 * PERFORMANCE CONSIDERATIONS:
 * - Cache frequently accessed paths to avoid database hits
 * - Batch operations to reduce transaction overhead
 * - Use database indexes for efficient path queries
 * - Implement path prefix compression for deep hierarchies
 */
public class NamespaceManager {
    
    private final MetadataStore metadataStore;
    private final ConcurrentHashMap<String, FileMetadata> pathCache;
    private final ReadWriteLock cacheLock;
    
    public NamespaceManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.pathCache = new ConcurrentHashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
    }
    
    /**
     * Normalize and validate a filesystem path
     * 
     * HDFS path requirements:
     * - Must start with /
     * - Cannot contain ".." or "."
     * - Cannot have consecutive slashes
     * - Cannot end with slash (except root "/")
     */
    public String normalizePath(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IOException("Path cannot be null or empty");
        }
        
        // TODO: Implement comprehensive path normalization
        // Handle cases like: //, /./, /../, trailing slashes
        throw new UnsupportedOperationException("Student implementation required");
    }
    
    /**
     * Get parent directory path
     */
    public String getParentPath(String path) {
        if ("/".equals(path)) {
            return null; // Root has no parent
        }
        
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == 0) {
            return "/"; // Parent is root
        }
        
        return path.substring(0, lastSlash);
    }
    
    /**
     * Get file/directory name from path
     */
    public String getFileName(String path) {
        if ("/".equals(path)) {
            return "";
        }
        
        int lastSlash = path.lastIndexOf('/');
        return path.substring(lastSlash + 1);
    }
    
    /**
     * Check if path exists in namespace
     */
    public boolean exists(String path) throws IOException {
        // Check cache first
        if (pathCache.containsKey(path)) {
            return true;
        }
        
        // Query metadata store
        Optional<FileMetadata> metadata = metadataStore.getMetadata(path);
        
        if (metadata.isPresent()) {
            // Update cache
            pathCache.put(path, metadata.get());
            return true;
        }
        
        return false;
    }
    
    /**
     * Validate that parent directories exist for a given path
     */
    public void validateParentExists(String path) throws IOException {
        String parentPath = getParentPath(path);
        if (parentPath != null && !exists(parentPath)) {
            throw new IOException("Parent directory does not exist: " + parentPath);
        }
    }
    
    /**
     * Invalidate cache entries for a path and all its children
     * Used during delete and rename operations
     */
    public void invalidateCache(String path) {
        cacheLock.writeLock().lock();
        try {
            // Remove exact path
            pathCache.remove(path);
            
            // Remove all child paths (prefix matching)
            String prefix = path.endsWith("/") ? path : path + "/";
            pathCache.entrySet().removeIf(entry -> 
                entry.getKey().startsWith(prefix));
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Get cached metadata if available
     */
    public Optional<FileMetadata> getCachedMetadata(String path) {
        return Optional.ofNullable(pathCache.get(path));
    }
    
    /**
     * Update cache with new metadata
     */
    public void updateCache(String path, FileMetadata metadata) {
        pathCache.put(path, metadata);
    }
    
    /**
     * Get cache statistics for monitoring
     */
    public CacheStats getCacheStats() {
        return new CacheStats(pathCache.size(), calculateHitRatio());
    }
    
    private double calculateHitRatio() {
        // TODO: Implement hit ratio calculation
        // Track cache hits vs misses over time
        return 0.0;
    }
    
    /**
     * Cache statistics for monitoring
     */
    public static class CacheStats {
        public final int size;
        public final double hitRatio;
        
        public CacheStats(int size, double hitRatio) {
            this.size = size;
            this.hitRatio = hitRatio;
        }
    }
}
