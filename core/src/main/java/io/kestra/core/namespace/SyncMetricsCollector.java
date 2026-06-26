package io.kestra.core.namespace;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects and aggregates metrics for sync operations.
 * 
 * This service tracks operation counts, durations, success/failure rates,
 * and other metrics useful for monitoring and debugging sync operations.
 */
@Singleton
@Slf4j
public class SyncMetricsCollector {
    
    /**
     * Metrics store keyed by namespace (tenantId/namespace).
     */
    private final Map<String, SyncMetrics> metricsStore = new ConcurrentHashMap<>();
    
    /**
     * Records a sync operation.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param operation the sync operation
     * @param durationMs the duration of the operation in milliseconds
     * @param success whether the operation was successful
     */
    public void recordOperation(String tenantId, String namespace, SyncOperation operation, 
                               long durationMs, boolean success) {
        try {
            String key = buildMetricsKey(tenantId, namespace);
            
            SyncMetrics metrics = metricsStore.computeIfAbsent(key, k -> SyncMetrics.builder()
                .totalOperations(0)
                .successfulOperations(0)
                .failedOperations(0)
                .averageDurationMs(0.0)
                .maxDurationMs(0)
                .minDurationMs(Long.MAX_VALUE)
                .successRate(0.0)
                .lastSyncTime(null)
                .build());
            
            // Update metrics
            metrics.recordOperation(durationMs, success);
            metrics.setLastSyncTime(Instant.now());
            
            log.debug("Recorded operation: {} for {}/{} - duration: {}ms, success: {}", 
                operation.getType(), tenantId, namespace, durationMs, success);
        } catch (Exception e) {
            log.warn("Failed to record operation metrics for {}/{}", tenantId, namespace, e);
        }
    }
    
    /**
     * Gets sync metrics for a namespace.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @return the sync metrics, or null if no metrics exist
     */
    public SyncMetrics getMetrics(String tenantId, String namespace) {
        String key = buildMetricsKey(tenantId, namespace);
        return metricsStore.get(key);
    }
    
    /**
     * Resets metrics for a namespace.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     */
    public void resetMetrics(String tenantId, String namespace) {
        try {
            String key = buildMetricsKey(tenantId, namespace);
            SyncMetrics metrics = metricsStore.get(key);
            
            if (metrics != null) {
                metrics.reset();
                log.debug("Reset metrics for {}/{}", tenantId, namespace);
            }
        } catch (Exception e) {
            log.warn("Failed to reset metrics for {}/{}", tenantId, namespace, e);
        }
    }
    
    /**
     * Gets all metrics.
     * 
     * @return map of all metrics keyed by namespace
     */
    public Map<String, SyncMetrics> getAllMetrics() {
        return new ConcurrentHashMap<>(metricsStore);
    }
    
    /**
     * Clears all metrics (useful for testing or cleanup).
     */
    public void clearAll() {
        metricsStore.clear();
        log.debug("Cleared all sync metrics");
    }
    
    /**
     * Builds a metrics key from tenant ID and namespace.
     */
    private String buildMetricsKey(String tenantId, String namespace) {
        return tenantId + "/" + namespace;
    }
}
