package io.kestra.core.namespace;

import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the state of sync operations for recovery and tracking purposes.
 * 
 * This service tracks pending, in-progress, and completed sync operations,
 * allowing for recovery of failed operations and monitoring of sync status.
 */
@Singleton
@Slf4j
public class SyncStateManager {
    
    /**
     * In-memory store for sync operations.
     * Key: operationId, Value: SyncOperation
     */
    private final Map<String, SyncOperation> operationStore = new ConcurrentHashMap<>();
    
    /**
     * Records the start of a sync operation.
     * 
     * @param operation the sync operation to record
     * @throws IOException if recording fails
     */
    public void recordSyncStart(SyncOperation operation) throws IOException {
        try {
            operation.setStatus(SyncOperation.OperationStatus.IN_PROGRESS);
            operation.setCreatedAt(Instant.now());
            operationStore.put(operation.getId(), operation);
            
            log.debug("Recorded sync start: {} for {}/{}/{}", 
                operation.getId(), operation.getTenantId(), operation.getNamespace(), operation.getFilePath());
        } catch (Exception e) {
            log.error("Failed to record sync start: {}", operation.getId(), e);
            throw new IOException("Failed to record sync start", e);
        }
    }
    
    /**
     * Records successful completion of a sync operation.
     * 
     * @param operationId the ID of the operation
     * @param durationMs the duration of the operation in milliseconds
     * @throws IOException if recording fails
     */
    public void recordSyncSuccess(String operationId, long durationMs) throws IOException {
        try {
            SyncOperation operation = operationStore.get(operationId);
            if (operation != null) {
                operation.setStatus(SyncOperation.OperationStatus.SUCCESS);
                operation.setCompletedAt(Instant.now());
                operation.setDurationMs(durationMs);
                
                log.debug("Recorded sync success: {} in {}ms", operationId, durationMs);
            } else {
                log.warn("Operation not found for success recording: {}", operationId);
            }
        } catch (Exception e) {
            log.error("Failed to record sync success: {}", operationId, e);
            throw new IOException("Failed to record sync success", e);
        }
    }
    
    /**
     * Records failure of a sync operation.
     * 
     * @param operationId the ID of the operation
     * @param error the error that occurred
     * @throws IOException if recording fails
     */
    public void recordSyncFailure(String operationId, Exception error) throws IOException {
        try {
            SyncOperation operation = operationStore.get(operationId);
            if (operation != null) {
                operation.setStatus(SyncOperation.OperationStatus.FAILED);
                operation.setCompletedAt(Instant.now());
                operation.setErrorMessage(error.getMessage());
                operation.setRetryCount(operation.getRetryCount() + 1);
                
                log.debug("Recorded sync failure: {} - {}", operationId, error.getMessage());
            } else {
                log.warn("Operation not found for failure recording: {}", operationId);
            }
        } catch (Exception e) {
            log.error("Failed to record sync failure: {}", operationId, e);
            throw new IOException("Failed to record sync failure", e);
        }
    }
    
    /**
     * Retrieves pending sync operations for recovery.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @return list of pending operations
     * @throws IOException if retrieval fails
     */
    public List<SyncOperation> getPendingOperations(String tenantId, String namespace) throws IOException {
        try {
            List<SyncOperation> pendingOps = new ArrayList<>();
            
            for (SyncOperation operation : operationStore.values()) {
                if (operation.getTenantId().equals(tenantId) && 
                    operation.getNamespace().equals(namespace) &&
                    (operation.getStatus() == SyncOperation.OperationStatus.PENDING ||
                     operation.getStatus() == SyncOperation.OperationStatus.IN_PROGRESS)) {
                    pendingOps.add(operation);
                }
            }
            
            log.debug("Retrieved {} pending operations for {}/{}", pendingOps.size(), tenantId, namespace);
            return pendingOps;
        } catch (Exception e) {
            log.error("Failed to retrieve pending operations for {}/{}", tenantId, namespace, e);
            throw new IOException("Failed to retrieve pending operations", e);
        }
    }
    
    /**
     * Clears completed sync operations.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @throws IOException if clearing fails
     */
    public void clearCompletedOperations(String tenantId, String namespace) throws IOException {
        try {
            List<String> opsToRemove = new ArrayList<>();
            
            for (Map.Entry<String, SyncOperation> entry : operationStore.entrySet()) {
                SyncOperation operation = entry.getValue();
                if (operation.getTenantId().equals(tenantId) && 
                    operation.getNamespace().equals(namespace) &&
                    (operation.getStatus() == SyncOperation.OperationStatus.SUCCESS ||
                     operation.getStatus() == SyncOperation.OperationStatus.ROLLED_BACK)) {
                    opsToRemove.add(entry.getKey());
                }
            }
            
            for (String opId : opsToRemove) {
                operationStore.remove(opId);
            }
            
            log.debug("Cleared {} completed operations for {}/{}", opsToRemove.size(), tenantId, namespace);
        } catch (Exception e) {
            log.error("Failed to clear completed operations for {}/{}", tenantId, namespace, e);
            throw new IOException("Failed to clear completed operations", e);
        }
    }
    
    /**
     * Gets the status of a specific operation.
     * 
     * @param operationId the ID of the operation
     * @return the operation, or null if not found
     */
    public SyncOperation getOperation(String operationId) {
        return operationStore.get(operationId);
    }
    
    /**
     * Gets all operations for a namespace.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @return list of all operations for the namespace
     */
    public List<SyncOperation> getAllOperations(String tenantId, String namespace) {
        List<SyncOperation> ops = new ArrayList<>();
        
        for (SyncOperation operation : operationStore.values()) {
            if (operation.getTenantId().equals(tenantId) && 
                operation.getNamespace().equals(namespace)) {
                ops.add(operation);
            }
        }
        
        return ops;
    }
    
    /**
     * Clears all operations (useful for testing or cleanup).
     */
    public void clearAll() {
        operationStore.clear();
        log.debug("Cleared all sync operations");
    }
}
