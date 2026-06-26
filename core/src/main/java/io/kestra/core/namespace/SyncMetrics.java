package io.kestra.core.namespace;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents metrics for sync operations.
 * 
 * This class tracks aggregated metrics about sync operations including counts,
 * durations, success rates, and timing information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncMetrics {
    /**
     * Total number of sync operations performed.
     */
    @Builder.Default
    private long totalOperations = 0;
    
    /**
     * Number of successful sync operations.
     */
    @Builder.Default
    private long successfulOperations = 0;
    
    /**
     * Number of failed sync operations.
     */
    @Builder.Default
    private long failedOperations = 0;
    
    /**
     * Average duration of sync operations in milliseconds.
     */
    @Builder.Default
    private double averageDurationMs = 0.0;
    
    /**
     * Maximum duration of a sync operation in milliseconds.
     */
    @Builder.Default
    private long maxDurationMs = 0;
    
    /**
     * Minimum duration of a sync operation in milliseconds.
     */
    @Builder.Default
    private long minDurationMs = Long.MAX_VALUE;
    
    /**
     * Success rate as a percentage (0-100).
     */
    @Builder.Default
    private double successRate = 0.0;
    
    /**
     * Timestamp of the last sync operation.
     */
    private Instant lastSyncTime;

    /**
     * Calculates the success rate based on total and successful operations.
     *
     * @return success rate as a percentage (0-100)
     */
    public double calculateSuccessRate() {
        if (totalOperations == 0) {
            return 0.0;
        }
        return (successfulOperations * 100.0) / totalOperations;
    }

    /**
     * Calculates the failure rate based on total and failed operations.
     *
     * @return failure rate as a percentage (0-100)
     */
    public double calculateFailureRate() {
        if (totalOperations == 0) {
            return 0.0;
        }
        return (failedOperations * 100.0) / totalOperations;
    }

    /**
     * Resets all metrics to their initial values.
     */
    public void reset() {
        this.totalOperations = 0;
        this.successfulOperations = 0;
        this.failedOperations = 0;
        this.averageDurationMs = 0.0;
        this.maxDurationMs = 0;
        this.minDurationMs = Long.MAX_VALUE;
        this.successRate = 0.0;
        this.lastSyncTime = null;
    }

    /**
     * Updates metrics with a new operation result.
     *
     * @param durationMs duration of the operation in milliseconds
     * @param success whether the operation was successful
     */
    public void recordOperation(long durationMs, boolean success) {
        totalOperations++;
        
        if (success) {
            successfulOperations++;
        } else {
            failedOperations++;
        }
        
        // Update duration statistics
        if (durationMs > maxDurationMs) {
            maxDurationMs = durationMs;
        }
        if (durationMs < minDurationMs) {
            minDurationMs = durationMs;
        }
        
        // Update average duration
        averageDurationMs = (averageDurationMs * (totalOperations - 1) + durationMs) / totalOperations;
        
        // Update success rate
        successRate = calculateSuccessRate();
        
        // Update last sync time
        lastSyncTime = Instant.now();
    }
}
