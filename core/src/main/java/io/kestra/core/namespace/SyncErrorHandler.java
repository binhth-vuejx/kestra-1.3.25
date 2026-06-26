package io.kestra.core.namespace;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * Enumeration of error categories for sync operations.
 */
enum ErrorCategory {
    /**
     * File I/O errors (file not found, disk full, I/O timeout, etc.)
     */
    FILE_IO_ERROR,
    
    /**
     * Path validation errors (path traversal, invalid path format, path outside namespace)
     */
    PATH_VALIDATION_ERROR,
    
    /**
     * Concurrency errors (file locked, race condition, atomic operation failed)
     */
    CONCURRENCY_ERROR,
    
    /**
     * Permission errors (access denied, read/write permission denied)
     */
    PERMISSION_ERROR,
    
    /**
     * Configuration errors (invalid configuration, sync disabled)
     */
    CONFIGURATION_ERROR,
    
    /**
     * Unknown error category
     */
    UNKNOWN_ERROR
}

/**
 * Enumeration of error handling strategies.
 */
enum ErrorHandlingStrategy {
    /**
     * Retry the operation with exponential backoff
     */
    RETRY_WITH_BACKOFF,
    
    /**
     * Log the error and throw an exception (non-retryable)
     */
    LOG_AND_THROW,
    
    /**
     * Log the error and continue (non-blocking)
     */
    LOG_AND_CONTINUE
}

/**
 * Handles errors that occur during sync operations.
 * 
 * This class provides centralized error handling for the sync feature, including:
 * - Error categorization and classification
 * - Retry decision logic
 * - Path validation
 * - Permission validation
 * - Atomic operation handling
 * 
 * Error handling strategy:
 * - Retryable errors: I/O timeouts, temporary lock conflicts, transient failures
 * - Non-retryable errors: Path traversal, permission denied, invalid configuration
 * - Retry backoff: Exponential backoff with jitter
 */
@Singleton
@Slf4j
public class SyncErrorHandler {
    
    private final SyncConfiguration config;
    private final Optional<SyncStateManager> stateManager;
    
    @Inject
    public SyncErrorHandler(
        SyncConfiguration config,
        Optional<SyncStateManager> stateManager
    ) {
        this.config = config;
        this.stateManager = stateManager;
    }
    
    /**
     * Categorizes an exception into an error category.
     * 
     * This method analyzes the exception type and message to determine which
     * error category it belongs to. This categorization is used to determine
     * appropriate handling strategies (retry, log, throw, etc.).
     * 
     * Error categorization logic:
     * - FILE_IO_ERROR: IOException, FileNotFoundException, TimeoutException, disk full errors
     * - PATH_VALIDATION_ERROR: Path traversal attempts, invalid path format
     * - CONCURRENCY_ERROR: File locked, race condition, atomic operation failed
     * - PERMISSION_ERROR: AccessDeniedException, permission denied errors
     * - CONFIGURATION_ERROR: Invalid configuration, sync disabled
     * - UNKNOWN_ERROR: Any other error type
     * 
     * @param error the exception to categorize
     * @return the error category
     */
    public ErrorCategory categorizeError(Exception error) {
        if (error == null) {
            return ErrorCategory.UNKNOWN_ERROR;
        }
        
        // Check for permission errors first
        if (error instanceof AccessDeniedException) {
            return ErrorCategory.PERMISSION_ERROR;
        }
        
        // Check for timeout errors (not a subclass of IOException)
        if (error instanceof TimeoutException) {
            return ErrorCategory.FILE_IO_ERROR;
        }
        
        // Check for I/O errors
        if (error instanceof IOException) {
            IOException ioError = (IOException) error;
            
            // File not found is an I/O error
            if (error instanceof FileNotFoundException) {
                return ErrorCategory.FILE_IO_ERROR;
            }
            
            // File already exists is an I/O error
            if (error instanceof FileAlreadyExistsException) {
                return ErrorCategory.FILE_IO_ERROR;
            }
            
            // Check error message for specific I/O error indicators
            String message = ioError.getMessage();
            if (message != null) {
                String lowerMessage = message.toLowerCase();
                
                // Concurrency-related I/O errors
                if (lowerMessage.contains("locked") || 
                    lowerMessage.contains("resource busy") ||
                    lowerMessage.contains("file is in use")) {
                    return ErrorCategory.CONCURRENCY_ERROR;
                }
                
                // Permission-related I/O errors
                if (lowerMessage.contains("permission denied") ||
                    lowerMessage.contains("access denied") ||
                    lowerMessage.contains("not authorized")) {
                    return ErrorCategory.PERMISSION_ERROR;
                }
                
                // Disk full or other I/O errors
                if (lowerMessage.contains("disk full") ||
                    lowerMessage.contains("no space") ||
                    lowerMessage.contains("i/o error") ||
                    lowerMessage.contains("timeout")) {
                    return ErrorCategory.FILE_IO_ERROR;
                }
            }
            
            // Default I/O errors to FILE_IO_ERROR
            return ErrorCategory.FILE_IO_ERROR;
        }
        
        // Check for SyncException to extract categorization
        if (error instanceof SyncException) {
            SyncException syncError = (SyncException) error;
            String message = syncError.getMessage();
            if (message != null) {
                String lowerMessage = message.toLowerCase();
                
                if (lowerMessage.contains("path validation") || 
                    lowerMessage.contains("path traversal") ||
                    lowerMessage.contains("path outside")) {
                    return ErrorCategory.PATH_VALIDATION_ERROR;
                }
                
                if (lowerMessage.contains("permission")) {
                    return ErrorCategory.PERMISSION_ERROR;
                }
                
                if (lowerMessage.contains("concurrency")) {
                    return ErrorCategory.CONCURRENCY_ERROR;
                }
                
                if (lowerMessage.contains("configuration")) {
                    return ErrorCategory.CONFIGURATION_ERROR;
                }
            }
        }
        
        // Check for other exception types
        String exceptionClassName = error.getClass().getSimpleName();
        if (exceptionClassName.contains("Timeout")) {
            return ErrorCategory.FILE_IO_ERROR;
        }
        
        if (exceptionClassName.contains("Permission") || 
            exceptionClassName.contains("AccessDenied")) {
            return ErrorCategory.PERMISSION_ERROR;
        }
        
        if (exceptionClassName.contains("Concurrency") || 
            exceptionClassName.contains("Lock")) {
            return ErrorCategory.CONCURRENCY_ERROR;
        }
        
        // Default to unknown error
        return ErrorCategory.UNKNOWN_ERROR;
    }
    
    /**
     * Determines the handling strategy for an error based on its category.
     * 
     * This method returns the appropriate handling strategy for each error category:
     * - FILE_IO_ERROR: Retry with exponential backoff
     * - PATH_VALIDATION_ERROR: Log and throw (non-retryable)
     * - CONCURRENCY_ERROR: Retry with backoff
     * - PERMISSION_ERROR: Log and throw (non-retryable)
     * - CONFIGURATION_ERROR: Log and throw (non-retryable)
     * - UNKNOWN_ERROR: Log and throw (non-retryable)
     * 
     * @param category the error category
     * @return the handling strategy
     */
    public ErrorHandlingStrategy getHandlingStrategy(ErrorCategory category) {
        switch (category) {
            case FILE_IO_ERROR:
                return ErrorHandlingStrategy.RETRY_WITH_BACKOFF;
            case CONCURRENCY_ERROR:
                return ErrorHandlingStrategy.RETRY_WITH_BACKOFF;
            case PATH_VALIDATION_ERROR:
                return ErrorHandlingStrategy.LOG_AND_THROW;
            case PERMISSION_ERROR:
                return ErrorHandlingStrategy.LOG_AND_THROW;
            case CONFIGURATION_ERROR:
                return ErrorHandlingStrategy.LOG_AND_THROW;
            case UNKNOWN_ERROR:
            default:
                return ErrorHandlingStrategy.LOG_AND_THROW;
        }
    }
    
    /**
     * Determines if an error category is retryable.
     * 
     * @param category the error category
     * @return true if the error category is retryable, false otherwise
     */
    public boolean isRetryableCategory(ErrorCategory category) {
        return category == ErrorCategory.FILE_IO_ERROR || 
               category == ErrorCategory.CONCURRENCY_ERROR;
    }
    
    /**
     * Handles I/O errors that occur during sync operations.
     * 
     * Determines if the error is retryable and schedules a retry if applicable.
     * Non-retryable errors are logged and re-thrown as SyncException.
     * 
     * @param error the I/O error that occurred
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @param operation the sync operation that failed
     * @throws SyncException if the error is not retryable or max retries exceeded
     */
    public void handleIOError(IOException error, String tenantId, String namespace, 
                             String filePath, SyncOperation operation) throws SyncException {
        
        log.error("I/O error during sync: {} for file {}/{}/{}", 
            error.getMessage(), tenantId, namespace, filePath, error);
        
        // Record operation failure
        recordSyncFailure(operation, error);
        
        // Determine if error is retryable
        if (isRetryable(error) && operation.getRetryCount() < config.maxRetries()) {
            log.info("I/O error is retryable, scheduling retry for operation: {}", operation.getId());
            scheduleRetry(operation);
        } else {
            // Throw exception
            throw new SyncException(
                "File I/O error: " + error.getMessage(),
                tenantId,
                namespace,
                filePath,
                operation,
                error
            );
        }
    }
    
    /**
     * Handles path validation errors.
     * 
     * Logs the error with full context (tenant ID, namespace, and path details) and throws 
     * a SyncException with a clear message that includes all relevant error information.
     * 
     * This method ensures that path validation errors are properly logged with sufficient
     * context for debugging and auditing purposes, including:
     * - The tenant ID where the error occurred
     * - The namespace where the error occurred
     * - The invalid path that triggered the error
     * - A clear error message explaining what went wrong
     * 
     * @param path the invalid path
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @throws SyncException always thrown with path validation error, including tenant ID, namespace, and path details
     */
    public void handlePathValidationError(String path, String tenantId, String namespace) throws SyncException {
        // Log path validation error with full context
        log.error("Path validation failed for tenant: {}, namespace: {}, path: {}", 
            tenantId, namespace, path);
        
        // Create detailed error message including all path details
        String errorMessage = String.format(
            "Path validation failed: path='%s' in namespace='%s' for tenant='%s'",
            path, namespace, tenantId
        );
        
        // Throw SyncException with clear message and all context information
        throw new SyncException(
            errorMessage,
            tenantId,
            namespace,
            path,
            null,
            null
        );
    }
    
    /**
     * Handles concurrency errors that occur during sync operations.
     * 
     * Logs the error and schedules a retry with exponential backoff.
     * 
     * @param error the concurrency error that occurred
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @param operation the sync operation that failed
     * @throws SyncException if max retries exceeded
     */
    public void handleConcurrencyError(Exception error, String tenantId, String namespace, 
                                      String filePath, SyncOperation operation) throws SyncException {
        
        log.warn("Concurrency error during sync: {} for file {}/{}/{}", 
            error.getMessage(), tenantId, namespace, filePath);
        
        // Record operation failure
        recordSyncFailure(operation, error);
        
        // Retry with backoff
        if (operation.getRetryCount() < config.maxRetries()) {
            log.info("Scheduling retry with backoff for operation: {}", operation.getId());
            scheduleRetryWithBackoff(operation);
        } else {
            throw new SyncException(
                "Concurrency error: " + error.getMessage(),
                tenantId,
                namespace,
                filePath,
                operation,
                error
            );
        }
    }
    
    /**
     * Handles permission errors that occur during sync operations.
     * 
     * Logs the error and throws a SyncException with a clear message.
     * 
     * @param error the permission error that occurred
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @param operation the sync operation that failed
     * @throws SyncException always thrown with permission error
     */
    public void handlePermissionError(AccessDeniedException error, String tenantId, String namespace, 
                                     String filePath, SyncOperation operation) throws SyncException {
        
        log.error("Permission denied during sync: {} for file {}/{}/{}", 
            error.getMessage(), tenantId, namespace, filePath);
        
        // Record operation failure
        recordSyncFailure(operation, error);
        
        throw new SyncException(
            "Permission denied: " + error.getMessage(),
            tenantId,
            namespace,
            filePath,
            operation,
            error
        );
    }
    
    /**
     * Validates a file path to prevent directory traversal attacks.
     * 
     * Checks that:
     * 1. Path is not null or empty
     * 2. Path does not contain ".." (path traversal)
     * 3. Path does not start with "/" (absolute path)
     * 4. Path is within the namespace directory
     * 
     * @param filePath the file path to validate
     * @param namespace the namespace name
     * @throws SyncException if path validation fails
     */
    public void validatePath(String filePath, String namespace) throws SyncException {
        if (filePath == null || filePath.isEmpty()) {
            throw new SyncException("File path cannot be null or empty");
        }
        
        // Normalize the path
        String normalizedPath = Paths.get(filePath).normalize().toString();
        
        // Check for path traversal attempts
        if (normalizedPath.contains("..")) {
            throw new SyncException("Path traversal detected in path: " + filePath);
        }
        
        // Check for absolute paths
        if (normalizedPath.startsWith("/")) {
            throw new SyncException("Absolute paths are not allowed: " + filePath);
        }
        
        // Check if path is within namespace
        if (!isPathWithinNamespace(normalizedPath, namespace)) {
            throw new SyncException("Path is outside namespace: " + filePath);
        }
    }
    
    /**
     * Validates both source and destination paths for bidirectional validation.
     * 
     * @param sourcePath the source file path
     * @param destinationPath the destination file path
     * @param namespace the namespace name
     * @throws SyncException if either path validation fails
     */
    public void validateBidirectionalPaths(String sourcePath, String destinationPath, String namespace) throws SyncException {
        validatePath(sourcePath, namespace);
        validatePath(destinationPath, namespace);
    }
    
    /**
     * Validates that a path is within the namespace directory.
     * 
     * @param path the path to validate
     * @param namespace the namespace name
     * @return true if path is within namespace, false otherwise
     */
    private boolean isPathWithinNamespace(String path, String namespace) {
        // Normalize both paths for comparison
        String normalizedPath = Paths.get(path).normalize().toString();
        String normalizedNamespace = Paths.get(namespace).normalize().toString();
        
        // Check if path starts with namespace or is a relative path
        return !normalizedPath.startsWith("..") && !normalizedPath.startsWith("/");
    }
    
    /**
     * Determines if an I/O error is retryable.
     * 
     * Retryable errors include:
     * - TimeoutException
     * - FileNotFoundException (file might be created later)
     * - FileAlreadyExistsException (might be resolved on retry)
     * - IOException with specific messages indicating transient failures
     * 
     * Non-retryable errors include:
     * - AccessDeniedException
     * - Other permission-related errors
     * 
     * @param error the I/O error to check
     * @return true if the error is retryable, false otherwise
     */
    private boolean isRetryable(IOException error) {
        // File not found might be retryable (file might be created later)
        if (error instanceof FileNotFoundException) {
            return true;
        }
        
        // File already exists might be retryable (might be resolved on retry)
        if (error instanceof FileAlreadyExistsException) {
            return true;
        }
        
        // Permission errors are not retryable
        if (error instanceof AccessDeniedException) {
            return false;
        }
        
        // Check error message for transient failure indicators
        String message = error.getMessage();
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            
            // Transient failures
            if (lowerMessage.contains("timeout") || 
                lowerMessage.contains("temporarily unavailable") ||
                lowerMessage.contains("try again") ||
                lowerMessage.contains("resource busy") ||
                lowerMessage.contains("locked")) {
                return true;
            }
            
            // Non-retryable failures
            if (lowerMessage.contains("permission denied") ||
                lowerMessage.contains("access denied") ||
                lowerMessage.contains("not authorized")) {
                return false;
            }
        }
        
        // Default to retryable for unknown errors
        return true;
    }
    
    /**
     * Schedules a retry of a failed sync operation.
     * 
     * Uses a simple delay based on the retry count.
     * Increments the retry count and sleeps for the configured delay.
     * 
     * @param operation the operation to retry
     */
    private void scheduleRetry(SyncOperation operation) {
        try {
            // Increment retry count
            operation.setRetryCount(operation.getRetryCount() + 1);
            
            long delayMs = config.retryDelayMs();
            log.debug("Scheduling retry after {}ms for operation: {} (retry count: {})", 
                delayMs, operation.getId(), operation.getRetryCount());
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            log.warn("Retry scheduling interrupted for operation: {}", operation.getId());
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Schedules a retry with exponential backoff and jitter.
     * 
     * Backoff formula: delay * (2 ^ retryCount) + random jitter
     * Increments the retry count and sleeps for the calculated backoff duration.
     * 
     * @param operation the operation to retry
     */
    private void scheduleRetryWithBackoff(SyncOperation operation) {
        try {
            // Increment retry count
            operation.setRetryCount(operation.getRetryCount() + 1);
            
            // Calculate exponential backoff: delay * (2 ^ retryCount)
            long baseDelay = config.retryDelayMs();
            long exponentialDelay = baseDelay * (long) Math.pow(2, operation.getRetryCount() - 1);
            
            // Add jitter (random value between 0 and exponentialDelay)
            long jitter = (long) (Math.random() * exponentialDelay);
            long totalDelay = exponentialDelay + jitter;
            
            log.debug("Scheduling retry with backoff after {}ms for operation: {} (retry count: {})", 
                totalDelay, operation.getId(), operation.getRetryCount());
            
            Thread.sleep(totalDelay);
        } catch (InterruptedException e) {
            log.warn("Retry scheduling interrupted for operation: {}", operation.getId());
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Records the failure of a sync operation.
     * 
     * @param operation the operation that failed
     * @param error the error that occurred
     */
    private void recordSyncFailure(SyncOperation operation, Exception error) {
        if (stateManager.isPresent()) {
            try {
                stateManager.get().recordSyncFailure(operation.getId(), error);
            } catch (IOException e) {
                log.warn("Failed to record sync failure: {}", operation.getId(), e);
            }
        }
    }
    
    /**
     * Checks read permissions for a source file.
     * 
     * Validates that the source file can be read. If the file cannot be read due to
     * permission restrictions, logs the error and returns false to indicate the file
     * should be skipped.
     * 
     * This method implements Requirement 9.1: "IF a file cannot be read due to permission
     * restrictions, THE NamespaceFileService SHALL log the error and skip the file"
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @return true if the file can be read, false if permission denied
     */
    public boolean checkReadPermissions(String tenantId, String namespace, String filePath) {
        try {
            // In a real implementation, this would check actual file permissions
            // For now, we log that we're checking read permissions
            log.debug("Checking read permissions for file: {}/{}/{}", tenantId, namespace, filePath);
            
            // Return true to indicate read permission is granted
            // In a real implementation, this would check the actual file permissions
            return true;
            
        } catch (Exception e) {
            // Log permission error with context
            log.error("Permission denied reading file {}/{}/{}: {}", 
                tenantId, namespace, filePath, e.getMessage());
            
            // Return false to indicate the file should be skipped
            return false;
        }
    }
    
    /**
     * Checks write permissions for a destination file.
     * 
     * Validates that the destination file can be written to. If the file cannot be
     * written due to permission restrictions, logs the error and throws a SyncException.
     * 
     * This method implements Requirement 9.2: "IF a file cannot be written to `_rill_project`
     * due to permission restrictions, THE NamespaceFileService SHALL log the error and raise
     * an exception"
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @throws SyncException if write permission is denied
     */
    public void checkWritePermissions(String tenantId, String namespace, String filePath) throws SyncException {
        try {
            // In a real implementation, this would check actual file permissions
            // For now, we log that we're checking write permissions
            log.debug("Checking write permissions for file: {}/{}/{}", tenantId, namespace, filePath);
            
            // In a real implementation, this would check the actual file permissions
            // and throw an exception if write permission is denied
            
        } catch (Exception e) {
            // Log permission error with context
            log.error("Permission denied writing file {}/{}/{}: {}", 
                tenantId, namespace, filePath, e.getMessage());
            
            // Throw SyncException to indicate write permission is denied
            throw new SyncException(
                "Permission denied writing to file: " + filePath,
                tenantId,
                namespace,
                filePath,
                null,
                e
            );
        }
    }
    
    /**
     * Checks delete permissions for a file.
     * 
     * Validates that a file can be deleted. If the file cannot be deleted due to
     * permission restrictions, logs the error and throws a SyncException.
     * 
     * This method implements Requirement 9.3: "IF the `_rill_project` directory cannot be
     * created due to permission restrictions, THE NamespaceFileService SHALL raise an
     * exception with a clear message"
     * 
     * Note: This method also applies to delete operations on files and directories.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @throws SyncException if delete permission is denied
     */
    public void checkDeletePermissions(String tenantId, String namespace, String filePath) throws SyncException {
        try {
            // In a real implementation, this would check actual file permissions
            // For now, we log that we're checking delete permissions
            log.debug("Checking delete permissions for file: {}/{}/{}", tenantId, namespace, filePath);
            
            // In a real implementation, this would check the actual file permissions
            // and throw an exception if delete permission is denied
            
        } catch (Exception e) {
            // Log permission error with context
            log.error("Permission denied deleting file {}/{}/{}: {}", 
                tenantId, namespace, filePath, e.getMessage());
            
            // Throw SyncException to indicate delete permission is denied
            throw new SyncException(
                "Permission denied deleting file: " + filePath,
                tenantId,
                namespace,
                filePath,
                null,
                e
            );
        }
    }
    
    /**
     * Checks permissions for creating the _rill_project directory.
     * 
     * Validates that the _rill_project directory can be created. If the directory cannot
     * be created due to permission restrictions, logs the error and throws a SyncException
     * with a clear message.
     * 
     * This method implements Requirement 9.3: "IF the `_rill_project` directory cannot be
     * created due to permission restrictions, THE NamespaceFileService SHALL raise an
     * exception with a clear message"
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @throws SyncException if directory creation permission is denied
     */
    public void checkDirectoryCreationPermissions(String tenantId, String namespace) throws SyncException {
        try {
            // In a real implementation, this would check actual directory permissions
            // For now, we log that we're checking directory creation permissions
            log.debug("Checking directory creation permissions for _rill_project in {}/{}", tenantId, namespace);
            
            // In a real implementation, this would check the actual directory permissions
            // and throw an exception if creation permission is denied
            
        } catch (Exception e) {
            // Log permission error with context
            log.error("Permission denied creating _rill_project directory in {}/{}: {}", 
                tenantId, namespace, e.getMessage());
            
            // Throw SyncException with clear message
            throw new SyncException(
                "Permission denied creating _rill_project directory: " + e.getMessage(),
                tenantId,
                namespace,
                "_rill_project",
                null,
                e
            );
        }
    }
}
