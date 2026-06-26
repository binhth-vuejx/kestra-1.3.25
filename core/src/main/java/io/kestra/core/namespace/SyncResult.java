package io.kestra.core.namespace;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the result of a full sync operation.
 * 
 * This class aggregates statistics about a sync operation including counts of
 * created, updated, and deleted files, as well as any errors that occurred.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncResult {
    /**
     * Tenant ID for the sync operation.
     */
    private String tenantId;
    
    /**
     * Namespace name for the sync operation.
     */
    private String namespace;
    
    /**
     * Number of files created during the sync.
     */
    @Builder.Default
    private int filesCreated = 0;
    
    /**
     * Number of files updated during the sync.
     */
    @Builder.Default
    private int filesUpdated = 0;
    
    /**
     * Number of files deleted during the sync.
     */
    @Builder.Default
    private int filesDeleted = 0;
    
    /**
     * Number of directories created during the sync.
     */
    @Builder.Default
    private int directoriesCreated = 0;
    
    /**
     * Number of directories deleted during the sync.
     */
    @Builder.Default
    private int directoriesDeleted = 0;
    
    /**
     * Total number of files processed during the sync.
     * This includes files created, updated, and deleted.
     */
    @Builder.Default
    private int filesProcessed = 0;
    
    /**
     * Total duration of the sync operation in milliseconds.
     */
    private long totalDurationMs;
    
    /**
     * List of errors that occurred during the sync.
     */
    @Builder.Default
    private List<SyncError> errors = new ArrayList<>();
    
    /**
     * Whether the sync operation was successful.
     */
    private boolean success;

    /**
     * Adds an error to the result.
     *
     * @param error the error to add
     */
    public void addError(SyncError error) {
        if (this.errors == null) {
            this.errors = new ArrayList<>();
        }
        this.errors.add(error);
    }

    /**
     * Gets the total number of operations performed.
     *
     * @return total number of files and directories created, updated, or deleted
     */
    public int getTotalOperations() {
        return filesCreated + filesUpdated + filesDeleted + directoriesCreated + directoriesDeleted;
    }

    /**
     * Gets the total number of files processed during the sync.
     * This includes all files that were examined, created, updated, or deleted.
     *
     * @return total number of files processed
     */
    public int getTotalFilesProcessed() {
        return filesProcessed;
    }

    /**
     * Gets the number of errors that occurred.
     *
     * @return number of errors
     */
    public int getErrorCount() {
        return errors != null ? errors.size() : 0;
    }

    /**
     * Checks if the sync operation had any errors.
     *
     * @return true if there were errors, false otherwise
     */
    public boolean hasErrors() {
        return getErrorCount() > 0;
    }
}
