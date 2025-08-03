package org.custom.hdfs.api;

import org.custom.hdfs.metadata.MetadataService;
import org.custom.hdfs.storage.MetadataStore.FileMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * WebHDFS-compatible REST API Controller
 * 
 * This controller provides HTTP endpoints that mirror the HDFS WebHDFS API,
 * allowing existing HDFS clients to work with the MinIO-backed metadata service.
 * 
 * WEBHDFS API COMPATIBILITY:
 * - GET /webhdfs/v1/{path}?op=LISTSTATUS (list directory)
 * - PUT /webhdfs/v1/{path}?op=MKDIRS (create directory)
 * - PUT /webhdfs/v1/{path}?op=CREATE (create file)
 * - DELETE /webhdfs/v1/{path}?op=DELETE (delete file/dir)
 * - PUT /webhdfs/v1/{path}?op=RENAME&destination={dest} (rename)
 * - GET /webhdfs/v1/{path}?op=GETFILESTATUS (get metadata)
 * 
 * LEARNING OBJECTIVES:
 * - Understand REST API design for distributed systems
 * - Learn HTTP status code semantics for filesystem operations
 * - Implement proper error handling and response formatting
 * - Handle authentication and authorization patterns
 * 
 * PERFORMANCE CONSIDERATIONS:
 * - Response streaming for large directory listings
 * - Async processing for long-running operations
 * - Request rate limiting and throttling
 * - Connection pooling and keepalive
 */
public class WebHDFSController {
    
    private final MetadataService metadataService;
    
    public WebHDFSController(MetadataService metadataService) {
        this.metadataService = metadataService;
    }
    
    /**
     * List directory contents
     * GET /webhdfs/v1/{path}?op=LISTSTATUS
     * 
     * PERFORMANCE NOTE:
     * This operation demonstrates the core difference between HDFS and object storage:
     * - HDFS: O(1) directory listing from NameNode memory
     * - Object Storage: O(n) prefix scan across distributed storage
     * - Our implementation: O(log n) with database indexing + caching
     */
    public WebHDFSResponse listStatus(String path) {
        try {
            List<FileMetadata> files = metadataService.listStatus(path);
            return WebHDFSResponse.success("FileStatuses", 
                new FileStatusesWrapper(files));
        } catch (IOException e) {
            return WebHDFSResponse.error("IOException", e.getMessage());
        } catch (Exception e) {
            return WebHDFSResponse.error("RemoteException", e.getMessage());
        }
    }
    
    /**
     * Create directory
     * PUT /webhdfs/v1/{path}?op=MKDIRS
     */
    public WebHDFSResponse mkdirs(String path, String user, String permission) {
        try {
            metadataService.mkdirs(path, user, "hdfs", permission);
            return WebHDFSResponse.success("boolean", true);
        } catch (IOException e) {
            return WebHDFSResponse.error("IOException", e.getMessage());
        }
    }
    
    /**
     * Create file
     * PUT /webhdfs/v1/{path}?op=CREATE
     */
    public WebHDFSResponse create(String path, long blockSize, short replication,
                                String user, String permission) {
        try {
            metadataService.createFile(path, blockSize, replication, user, "hdfs", permission);
            return WebHDFSResponse.success("Location", buildFileLocation(path));
        } catch (IOException e) {
            return WebHDFSResponse.error("IOException", e.getMessage());
        }
    }
    
    /**
     * Delete file or directory
     * DELETE /webhdfs/v1/{path}?op=DELETE&recursive={true|false}
     * 
     * COMPLEXITY ANALYSIS:
     * - File deletion: Simple metadata removal + async object cleanup
     * - Directory deletion: Potentially expensive recursive operation
     * - Large directory: May require background processing with status polling
     */
    public WebHDFSResponse delete(String path, boolean recursive) {
        try {
            metadataService.delete(path, recursive);
            return WebHDFSResponse.success("boolean", true);
        } catch (IOException e) {
            return WebHDFSResponse.error("IOException", e.getMessage());
        }
    }
    
    /**
     * Rename operation
     * PUT /webhdfs/v1/{source}?op=RENAME&destination={dest}
     * 
     * This is the most expensive operation in object storage scenarios.
     * Consider implementing:
     * - Async processing for large renames
     * - Progress tracking and status endpoints
     * - Rollback capabilities for failed operations
     */
    public WebHDFSResponse rename(String sourcePath, String destPath) {
        try {
            metadataService.rename(sourcePath, destPath);
            return WebHDFSResponse.success("boolean", true);
        } catch (IOException e) {
            return WebHDFSResponse.error("IOException", e.getMessage());
        }
    }
    
    /**
     * Get file status
     * GET /webhdfs/v1/{path}?op=GETFILESTATUS
     */
    public WebHDFSResponse getFileStatus(String path) {
        try {
            Optional<FileMetadata> metadata = metadataService.getFileStatus(path);
            if (metadata.isPresent()) {
                return WebHDFSResponse.success("FileStatus", metadata.get());
            } else {
                return WebHDFSResponse.error("FileNotFoundException", 
                    "File not found: " + path);
            }
        } catch (IOException e) {
            return WebHDFSResponse.error("IOException", e.getMessage());
        }
    }
    
    /**
     * Health check endpoint
     * GET /api/v1/health
     */
    public WebHDFSResponse health() {
        MetadataService.HealthStatus health = metadataService.getHealth();
        return WebHDFSResponse.success("health", health);
    }
    
    /**
     * Performance metrics endpoint
     * GET /api/v1/metrics
     */
    public WebHDFSResponse metrics() {
        MetadataService.PerformanceMetrics metrics = metadataService.getMetrics();
        return WebHDFSResponse.success("metrics", metrics);
    }
    
    private String buildFileLocation(String path) {
        // TODO: Build proper WebHDFS location URL
        return "http://localhost:8080/webhdfs/v1" + path;
    }
    
    /**
     * Standard WebHDFS response wrapper
     */
    public static class WebHDFSResponse {
        public final boolean success;
        public final String type;
        public final Object data;
        public final String errorType;
        public final String errorMessage;
        
        private WebHDFSResponse(boolean success, String type, Object data, 
                              String errorType, String errorMessage) {
            this.success = success;
            this.type = type;
            this.data = data;
            this.errorType = errorType;
            this.errorMessage = errorMessage;
        }
        
        public static WebHDFSResponse success(String type, Object data) {
            return new WebHDFSResponse(true, type, data, null, null);
        }
        
        public static WebHDFSResponse error(String errorType, String message) {
            return new WebHDFSResponse(false, null, null, errorType, message);
        }
    }
    
    /**
     * Wrapper for FileStatuses response (WebHDFS compatibility)
     */
    public static class FileStatusesWrapper {
        public final List<FileMetadata> FileStatus;
        
        public FileStatusesWrapper(List<FileMetadata> fileStatuses) {
            this.FileStatus = fileStatuses;
        }
    }
}
