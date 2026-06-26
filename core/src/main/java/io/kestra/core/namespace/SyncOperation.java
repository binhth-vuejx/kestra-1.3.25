package io.kestra.core.namespace;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a single sync operation.
 * 
 * This class tracks the details of a sync operation including its type, status,
 * timing information, and any errors that occurred.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncOperation {
    /**
     * Unique identifier for the sync operation.
     */
    private String id;
    
    /**
     * Tenant ID for the operation.
     */
    private String tenantId;
    
    /**
     * Namespace name for the operation.
     */
    private String namespace;
    
    /**
     * File path being synced.
     */
    private String filePath;
    
    /**
     * Type of operation (CREATE, UPDATE, DELETE, etc.).
     */
    private OperationType type;
    
    /**
     * Current status of the operation.
     */
    private OperationStatus status;
    
    /**
     * Timestamp when the operation was created.
     */
    private Instant createdAt;
    
    /**
     * Timestamp when the operation completed.
     */
    private Instant completedAt;
    
    /**
     * Duration of the operation in milliseconds.
     */
    private Long durationMs;
    
    /**
     * Error message if the operation failed.
     */
    private String errorMessage;
    
    /**
     * Number of retry attempts made.
     */
    private int retryCount;

    /**
     * Enum representing the type of sync operation.
     */
    public enum OperationType {
        /**
         * File creation operation.
         */
        FILE_CREATE,
        
        /**
         * File update operation.
         */
        FILE_UPDATE,
        
        /**
         * File deletion operation.
         */
        FILE_DELETE,
        
        /**
         * Directory creation operation.
         */
        DIR_CREATE,
        
        /**
         * Directory deletion operation.
         */
        DIR_DELETE
    }

    /**
     * Enum representing the status of a sync operation.
     */
    public enum OperationStatus {
        /**
         * Operation is pending execution.
         */
        PENDING,
        
        /**
         * Operation is currently in progress.
         */
        IN_PROGRESS,
        
        /**
         * Operation completed successfully.
         */
        SUCCESS,
        
        /**
         * Operation failed.
         */
        FAILED,
        
        /**
         * Operation was rolled back.
         */
        ROLLED_BACK
    }
}
