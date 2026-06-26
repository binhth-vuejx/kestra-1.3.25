package io.kestra.core.namespace;

import lombok.Getter;

/**
 * Custom exception for sync operation errors.
 * 
 * This exception is thrown when a sync operation fails, providing detailed context
 * about the error including tenant, namespace, file path, and operation details.
 */
@Getter
public class SyncException extends Exception {
    private final String tenantId;
    private final String namespace;
    private final String filePath;
    private final SyncOperation operation;
    private final Throwable cause;

    /**
     * Constructs a SyncException with all context information.
     *
     * @param message the error message
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @param operation the sync operation that failed
     * @param cause the underlying cause of the error
     */
    public SyncException(String message, String tenantId, String namespace, 
                        String filePath, SyncOperation operation, Throwable cause) {
        super(formatMessage(message, tenantId, namespace, filePath, operation), cause);
        this.tenantId = tenantId;
        this.namespace = namespace;
        this.filePath = filePath;
        this.operation = operation;
        this.cause = cause;
    }

    /**
     * Constructs a SyncException with message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause of the error
     */
    public SyncException(String message, Throwable cause) {
        super(message, cause);
        this.tenantId = null;
        this.namespace = null;
        this.filePath = null;
        this.operation = null;
        this.cause = cause;
    }

    /**
     * Constructs a SyncException with only a message.
     *
     * @param message the error message
     */
    public SyncException(String message) {
        super(message);
        this.tenantId = null;
        this.namespace = null;
        this.filePath = null;
        this.operation = null;
        this.cause = null;
    }

    /**
     * Formats the error message with context information.
     *
     * @param message the base error message
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @param operation the sync operation
     * @return formatted error message
     */
    private static String formatMessage(String message, String tenantId, String namespace,
                                        String filePath, SyncOperation operation) {
        StringBuilder sb = new StringBuilder(message);
        
        if (tenantId != null) {
            sb.append(" [tenantId=").append(tenantId);
        }
        
        if (namespace != null) {
            if (tenantId == null) {
                sb.append(" [");
            } else {
                sb.append(", ");
            }
            sb.append("namespace=").append(namespace);
        }
        
        if (filePath != null) {
            if (tenantId == null && namespace == null) {
                sb.append(" [");
            } else {
                sb.append(", ");
            }
            sb.append("filePath=").append(filePath);
        }
        
        if (operation != null) {
            if (tenantId == null && namespace == null && filePath == null) {
                sb.append(" [");
            } else {
                sb.append(", ");
            }
            sb.append("operation=").append(operation.getType());
        }
        
        if (tenantId != null || namespace != null || filePath != null || operation != null) {
            sb.append("]");
        }
        
        return sb.toString();
    }
}
