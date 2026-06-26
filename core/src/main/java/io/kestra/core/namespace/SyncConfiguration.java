package io.kestra.core.namespace;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Configuration for the Rill Project Sync feature.
 * 
 * This configuration controls the behavior of automatic synchronization of namespace files
 * to the non-versioned _rill_project directory.
 */
@ConfigurationProperties("kestra.namespace-files.sync")
public record SyncConfiguration(
    /**
     * Enable/disable automatic syncing.
     * Default: true
     */
    @Bindable(defaultValue = "true")
    @NotNull
    Boolean enabled,
    
    /**
     * Maximum file size for sync (bytes).
     * Default: 100MB (104857600 bytes)
     */
    @Bindable(defaultValue = "104857600")
    @Min(1)
    @NotNull
    Long maxFileSizeBytes,
    
    /**
     * Timeout for sync operations (milliseconds).
     * Default: 30 seconds (30000 ms)
     */
    @Bindable(defaultValue = "30000")
    @Min(1)
    @NotNull
    Long operationTimeoutMs,
    
    /**
     * Performance warning threshold (milliseconds).
     * Operations exceeding this duration will log a warning.
     * Default: 5 seconds (5000 ms)
     */
    @Bindable(defaultValue = "5000")
    @Min(1)
    @NotNull
    Long performanceWarningThresholdMs,
    
    /**
     * Number of retry attempts for failed operations.
     * Default: 3
     */
    @Bindable(defaultValue = "3")
    @Min(0)
    @NotNull
    Integer maxRetries,
    
    /**
     * Delay between retry attempts (milliseconds).
     * Default: 100 ms
     */
    @Bindable(defaultValue = "100")
    @Min(0)
    @NotNull
    Long retryDelayMs,
    
    /**
     * Enable atomic operations (use temp files + rename).
     * Default: true
     */
    @Bindable(defaultValue = "true")
    @NotNull
    Boolean useAtomicOperations,
    
    /**
     * Enable concurrent sync operations.
     * Default: true
     */
    @Bindable(defaultValue = "true")
    @NotNull
    Boolean enableConcurrentSync,
    
    /**
     * Maximum concurrent sync operations.
     * Default: 10
     */
    @Bindable(defaultValue = "10")
    @Min(1)
    @NotNull
    Integer maxConcurrentOperations
) {
    /**
     * Validates the configuration values.
     * 
     * @throws IllegalArgumentException if configuration values are invalid
     */
    public void validate() {
        if (maxFileSizeBytes != null && maxFileSizeBytes <= 0) {
            throw new IllegalArgumentException("maxFileSizeBytes must be greater than 0");
        }
        
        if (operationTimeoutMs != null && operationTimeoutMs <= 0) {
            throw new IllegalArgumentException("operationTimeoutMs must be greater than 0");
        }
        
        if (performanceWarningThresholdMs != null && performanceWarningThresholdMs <= 0) {
            throw new IllegalArgumentException("performanceWarningThresholdMs must be greater than 0");
        }
        
        if (maxRetries != null && maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be greater than or equal to 0");
        }
        
        if (retryDelayMs != null && retryDelayMs < 0) {
            throw new IllegalArgumentException("retryDelayMs must be greater than or equal to 0");
        }
        
        if (maxConcurrentOperations != null && maxConcurrentOperations <= 0) {
            throw new IllegalArgumentException("maxConcurrentOperations must be greater than 0");
        }
        
        // Validate logical relationships
        if (performanceWarningThresholdMs != null && operationTimeoutMs != null 
            && performanceWarningThresholdMs > operationTimeoutMs) {
            throw new IllegalArgumentException(
                "performanceWarningThresholdMs must be less than or equal to operationTimeoutMs");
        }
    }
}
