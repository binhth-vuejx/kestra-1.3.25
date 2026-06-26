package io.kestra.core.namespace;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents an error that occurred during a sync operation.
 * 
 * This class tracks error details including the file path, operation type,
 * error message, and timestamp.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncError {
    /**
     * File path where the error occurred.
     */
    private String filePath;
    
    /**
     * Type of operation that failed.
     */
    private String operation;
    
    /**
     * Error message describing what went wrong.
     */
    private String errorMessage;
    
    /**
     * Timestamp when the error occurred.
     */
    private Instant timestamp;
}
