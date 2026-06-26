package io.kestra.core.namespace;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.kestra.core.models.namespaces.files.NamespaceFileMetadata;
import io.kestra.core.repositories.NamespaceFileMetadataRepositoryInterface;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Core service responsible for synchronizing namespace files to the _rill_project directory.
 * 
 * This service handles all sync operations including file creation, update, deletion,
 * and directory operations. It manages atomic file operations, error handling, and
 * state tracking for sync operations.
 */
@Singleton
@Slf4j
public class RillProjectSyncService {

    private static final String RILL_PROJECT_DIR = "_rill_project";

    private final StorageInterface storage;
    private final SyncConfiguration config;
    private final Optional<SyncStateManager> stateManager;
    private final Optional<SyncMetricsCollector> metricsCollector;
    private final Optional<NamespaceFileMetadataRepositoryInterface> metadataRepository;

    @Inject
    public RillProjectSyncService(
        StorageInterface storage,
        SyncConfiguration config,
        Optional<SyncStateManager> stateManager,
        Optional<SyncMetricsCollector> metricsCollector,
        Optional<NamespaceFileMetadataRepositoryInterface> metadataRepository) {
        this.storage = storage;
        this.config = config;
        this.stateManager = stateManager;
        this.metricsCollector = metricsCollector;
        this.metadataRepository = metadataRepository;
    }

    /**
     * Gets the storage interface for testing purposes.
     * 
     * @return the storage interface
     */
    protected StorageInterface getStorage() {
        return storage;
    }

    /**
     * Initializes the _rill_project directory for a namespace.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @throws IOException if directory creation fails
     * @throws SyncException if sync operation fails
     */
    public void initializeRillProject(String tenantId, String namespace) throws IOException, SyncException {
        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping _rill_project initialization");
            return;
        }

        try {
            log.info("Initializing _rill_project directory for namespace: {}/{}", tenantId, namespace);

            URI rillProjectUri = buildRillProjectUri(tenantId, namespace);

            // Check if directory already exists
            if (storage.exists(tenantId, namespace, rillProjectUri)) {
                log.debug("_rill_project directory already exists for namespace: {}/{}", tenantId, namespace);
                return;
            }

            // Create the directory
            storage.createDirectory(tenantId, namespace, rillProjectUri);
            log.info("Successfully initialized _rill_project directory for namespace: {}/{}", tenantId, namespace);

        } catch (IOException e) {
            log.error("Failed to initialize _rill_project directory for namespace: {}/{}", tenantId, namespace, e);
            throw new SyncException(
                "Failed to initialize _rill_project directory",
                tenantId,
                namespace,
                RILL_PROJECT_DIR,
                null,
                e
            );
        }
    }

    /**
     * Syncs a newly created file to _rill_project.
     * 
     * Uses atomic file operations (temp file + rename) to ensure consistency.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @param content the file content
     * @throws IOException if sync operation fails
     * @throws SyncException if sync operation fails
     */
    public void syncFileCreate(String tenantId, String namespace, String filePath, InputStream content)
        throws IOException, SyncException {

        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping file create sync");
            return;
        }

        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();
        SyncOperation operation = SyncOperation.builder()
            .id(operationId)
            .tenantId(tenantId)
            .namespace(namespace)
            .filePath(filePath)
            .type(SyncOperation.OperationType.FILE_CREATE)
            .status(SyncOperation.OperationStatus.PENDING)
            .createdAt(Instant.now())
            .retryCount(0)
            .build();

        try {
            // Record operation start
            recordSyncStart(operation);

            // Validate path
            validatePath(filePath, namespace);

            log.debug("Syncing file create: {}/{}/{}", tenantId, namespace, filePath);

            // Ensure _rill_project directory exists
            ensureRillProjectDirectory(tenantId, namespace);

            // Create parent directories if needed
            createParentDirectories(tenantId, namespace, filePath);

            // Copy file to _rill_project using atomic operations
            URI rillProjectFilePath = buildRillProjectFilePath(tenantId, namespace, filePath);
            atomicPutFile(tenantId, namespace, rillProjectFilePath, content);

            // Update metadata after successful sync
            updateMetadataAfterSync(tenantId, namespace, filePath);

            // Record success
            long duration = System.currentTimeMillis() - startTime;
            recordSyncSuccess(operation, duration);

            // Log performance warning if needed
            if (duration > config.performanceWarningThresholdMs()) {
                log.warn(
                    "File create sync took longer than expected: {}ms for {}/{}/{}",
                    duration, tenantId, namespace, filePath
                );
            }

            log.info("Successfully synced file create: {}/{}/{}", tenantId, namespace, filePath);

        } catch (IOException e) {
            long duration = System.currentTimeMillis() - startTime;
            recordSyncFailure(operation, e, duration);
            log.error("Failed to sync file create: {}/{}/{}", tenantId, namespace, filePath, e);
            throw new SyncException(
                "Failed to sync file create",
                tenantId,
                namespace,
                filePath,
                operation,
                e
            );
        }
    }

    /**
     * Syncs an updated file to _rill_project.
     * 
     * Uses atomic file operations (temp file + rename) to ensure consistency.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @param content the file content
     * @throws IOException if sync operation fails
     * @throws SyncException if sync operation fails
     */
    public void syncFileUpdate(String tenantId, String namespace, String filePath, InputStream content)
        throws IOException, SyncException {

        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping file update sync");
            return;
        }

        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();
        SyncOperation operation = SyncOperation.builder()
            .id(operationId)
            .tenantId(tenantId)
            .namespace(namespace)
            .filePath(filePath)
            .type(SyncOperation.OperationType.FILE_UPDATE)
            .status(SyncOperation.OperationStatus.PENDING)
            .createdAt(Instant.now())
            .retryCount(0)
            .build();

        try {
            // Record operation start
            recordSyncStart(operation);

            // Validate path
            validatePath(filePath, namespace);

            log.debug("Syncing file update: {}/{}/{}", tenantId, namespace, filePath);

            // Ensure _rill_project directory exists
            ensureRillProjectDirectory(tenantId, namespace);

            // Create parent directories if needed
            createParentDirectories(tenantId, namespace, filePath);

            // Update file in _rill_project using atomic operations
            URI rillProjectFilePath = buildRillProjectFilePath(tenantId, namespace, filePath);
            atomicPutFile(tenantId, namespace, rillProjectFilePath, content);

            // Update metadata after successful sync
            updateMetadataAfterSync(tenantId, namespace, filePath);

            // Record success
            long duration = System.currentTimeMillis() - startTime;
            recordSyncSuccess(operation, duration);

            // Log performance warning if needed
            if (duration > config.performanceWarningThresholdMs()) {
                log.warn(
                    "File update sync took longer than expected: {}ms for {}/{}/{}",
                    duration, tenantId, namespace, filePath
                );
            }

            log.info("Successfully synced file update: {}/{}/{}", tenantId, namespace, filePath);

        } catch (IOException e) {
            long duration = System.currentTimeMillis() - startTime;
            recordSyncFailure(operation, e, duration);
            log.error("Failed to sync file update: {}/{}/{}", tenantId, namespace, filePath, e);
            throw new SyncException(
                "Failed to sync file update",
                tenantId,
                namespace,
                filePath,
                operation,
                e
            );
        }
    }

    /**
     * Syncs a deleted file removal from _rill_project.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     * @throws IOException if sync operation fails
     * @throws SyncException if sync operation fails
     */
    public void syncFileDelete(String tenantId, String namespace, String filePath)
        throws IOException, SyncException {

        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping file delete sync");
            return;
        }

        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();
        SyncOperation operation = SyncOperation.builder()
            .id(operationId)
            .tenantId(tenantId)
            .namespace(namespace)
            .filePath(filePath)
            .type(SyncOperation.OperationType.FILE_DELETE)
            .status(SyncOperation.OperationStatus.PENDING)
            .createdAt(Instant.now())
            .retryCount(0)
            .build();

        try {
            // Record operation start
            recordSyncStart(operation);

            // Validate path
            validatePath(filePath, namespace);

            log.debug("Syncing file delete: {}/{}/{}", tenantId, namespace, filePath);

            // Delete file from _rill_project
            URI rillProjectFilePath = buildRillProjectFilePath(tenantId, namespace, filePath);

            // Delete the file (ignore if it doesn't exist)
            try {
                storage.delete(tenantId, namespace, rillProjectFilePath);
            } catch (IOException e) {
                // File might not exist in _rill_project, which is fine
                log.debug("File not found in _rill_project during delete: {}/{}/{}", tenantId, namespace, filePath);
            }

            // Update metadata after successful sync
            updateMetadataAfterSync(tenantId, namespace, filePath);

            // Clean up empty parent directories
            cleanupEmptyDirectories(tenantId, namespace, filePath);

            // Record success
            long duration = System.currentTimeMillis() - startTime;
            recordSyncSuccess(operation, duration);

            // Log performance warning if needed
            if (duration > config.performanceWarningThresholdMs()) {
                log.warn(
                    "File delete sync took longer than expected: {}ms for {}/{}/{}",
                    duration, tenantId, namespace, filePath
                );
            }

            log.info("Successfully synced file delete: {}/{}/{}", tenantId, namespace, filePath);

        } catch (IOException e) {
            long duration = System.currentTimeMillis() - startTime;
            recordSyncFailure(operation, e, duration);
            log.error("Failed to sync file delete: {}/{}/{}", tenantId, namespace, filePath, e);
            throw new SyncException(
                "Failed to sync file delete",
                tenantId,
                namespace,
                filePath,
                operation,
                e
            );
        }
    }

    /**
     * Syncs a created directory to _rill_project.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param dirPath the directory path
     * @throws IOException if sync operation fails
     * @throws SyncException if sync operation fails
     */
    public void syncDirectoryCreate(String tenantId, String namespace, String dirPath)
        throws IOException, SyncException {

        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping directory create sync");
            return;
        }

        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();
        SyncOperation operation = SyncOperation.builder()
            .id(operationId)
            .tenantId(tenantId)
            .namespace(namespace)
            .filePath(dirPath)
            .type(SyncOperation.OperationType.DIR_CREATE)
            .status(SyncOperation.OperationStatus.PENDING)
            .createdAt(Instant.now())
            .retryCount(0)
            .build();

        try {
            // Record operation start
            recordSyncStart(operation);

            // Validate path
            validatePath(dirPath, namespace);

            log.debug("Syncing directory create: {}/{}/{}", tenantId, namespace, dirPath);

            // Ensure _rill_project directory exists
            ensureRillProjectDirectory(tenantId, namespace);

            // Create directory in _rill_project
            URI rillProjectDirPath = buildRillProjectFilePath(tenantId, namespace, dirPath);

            // Check if directory already exists
            if (!storage.exists(tenantId, namespace, rillProjectDirPath)) {
                storage.createDirectory(tenantId, namespace, rillProjectDirPath);
            }

            // Record success
            long duration = System.currentTimeMillis() - startTime;
            recordSyncSuccess(operation, duration);

            // Log performance warning if needed
            if (duration > config.performanceWarningThresholdMs()) {
                log.warn(
                    "Directory create sync took longer than expected: {}ms for {}/{}/{}",
                    duration, tenantId, namespace, dirPath
                );
            }

            log.info("Successfully synced directory create: {}/{}/{}", tenantId, namespace, dirPath);

        } catch (IOException e) {
            long duration = System.currentTimeMillis() - startTime;
            recordSyncFailure(operation, e, duration);
            log.error("Failed to sync directory create: {}/{}/{}", tenantId, namespace, dirPath, e);
            throw new SyncException(
                "Failed to sync directory create",
                tenantId,
                namespace,
                dirPath,
                operation,
                e
            );
        }
    }

    /**
     * Syncs a deleted directory removal from _rill_project.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param dirPath the directory path
     * @throws IOException if sync operation fails
     * @throws SyncException if sync operation fails
     */
    public void syncDirectoryDelete(String tenantId, String namespace, String dirPath)
        throws IOException, SyncException {

        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping directory delete sync");
            return;
        }

        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();
        SyncOperation operation = SyncOperation.builder()
            .id(operationId)
            .tenantId(tenantId)
            .namespace(namespace)
            .filePath(dirPath)
            .type(SyncOperation.OperationType.DIR_DELETE)
            .status(SyncOperation.OperationStatus.PENDING)
            .createdAt(Instant.now())
            .retryCount(0)
            .build();

        // Record operation start
        recordSyncStart(operation);

        // Validate path
        validatePath(dirPath, namespace);

        log.debug("Syncing directory delete: {}/{}/{}", tenantId, namespace, dirPath);

        // Delete directory and all contents from _rill_project
        URI rillProjectDirPath = buildRillProjectFilePath(tenantId, namespace, dirPath);
        // Ensure dirPath ends with "/" for proper directory delete
        if (!dirPath.endsWith("/")) {
            rillProjectDirPath = buildRillProjectFilePath(tenantId, namespace, dirPath + "/");
        }

        // Delete all files under this directory
        try {
            List<URI> filesToDelete = storage.allByPrefix(tenantId, namespace, rillProjectDirPath, true);
            for (URI fileUri : filesToDelete) {
                try {
                    storage.delete(tenantId, namespace, fileUri);
                } catch (IOException e) {
                    log.debug("Failed to delete file during directory delete: {}", fileUri, e);
                }
            }
        } catch (IOException e) {
            // Directory might not exist, which is fine
            log.debug("Directory not found in _rill_project during delete: {}/{}/{}", tenantId, namespace, dirPath);
        }

        // Also try to delete the directory itself (and any subdirs) using deleteByPrefix
        try {
            storage.deleteByPrefix(tenantId, namespace, rillProjectDirPath);
        } catch (IOException e) {
            log.debug("Failed to deleteByPrefix during directory delete: {}/{}/{}", tenantId, namespace, dirPath, e);
        }

        // Record success
        long duration = System.currentTimeMillis() - startTime;
        recordSyncSuccess(operation, duration);

        // Log performance warning if needed
        if (duration > config.performanceWarningThresholdMs()) {
            log.warn(
                "Directory delete sync took longer than expected: {}ms for {}/{}/{}",
                duration, tenantId, namespace, dirPath
            );
        }

        log.info("Successfully synced directory delete: {}/{}/{}", tenantId, namespace, dirPath);
    }

    /**
     * Performs a full sync of all files in a namespace to _rill_project.
     * 
     * This method implements the complete full sync workflow:
     * 1. Get list of files in latest version
     * 2. Get list of files in current _rill_project
     * 3. Compare files and identify changes
     * 4. Sync new and updated files
     * 5. Delete removed files
     * 6. Clean up empty directories
     * 7. Return SyncResult with statistics
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @return the result of the full sync operation
     * @throws IOException if sync operation fails
     * @throws SyncException if sync operation fails
     */
    public SyncResult fullSync(String tenantId, String namespace) throws IOException, SyncException {
        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping full sync");
            return SyncResult.builder()
                .tenantId(tenantId)
                .namespace(namespace)
                .filesCreated(0)
                .filesUpdated(0)
                .filesDeleted(0)
                .directoriesCreated(0)
                .directoriesDeleted(0)
                .filesProcessed(0)
                .totalDurationMs(0)
                .errors(new ArrayList<>())
                .success(true)
                .build();
        }

        long startTime = System.currentTimeMillis();
        log.info("Starting full sync for namespace: {}/{}", tenantId, namespace);

        SyncResult.SyncResultBuilder resultBuilder = SyncResult.builder()
            .tenantId(tenantId)
            .namespace(namespace)
            .filesCreated(0)
            .filesUpdated(0)
            .filesDeleted(0)
            .directoriesCreated(0)
            .directoriesDeleted(0)
            .filesProcessed(0)
            .errors(new ArrayList<>());

        try {
            // Ensure _rill_project directory exists
            ensureRillProjectDirectory(tenantId, namespace);

            // Task 6.1: Get list of files in latest version
            List<FileInfo> latestVersionFiles = getLatestVersionFiles(tenantId, namespace);
            log.debug(
                "Found {} files in latest version for namespace: {}/{}",
                latestVersionFiles.size(), tenantId, namespace
            );

            // Task 6.2: Get list of files in current _rill_project
            List<FileInfo> rillProjectFiles = getRillProjectFiles(tenantId, namespace);
            log.debug(
                "Found {} files in _rill_project for namespace: {}/{}",
                rillProjectFiles.size(), tenantId, namespace
            );

            // Task 6.3: Compare files and identify changes
            FileComparison comparison = compareFiles(latestVersionFiles, rillProjectFiles);
            log.debug(
                "File comparison results - New: {}, Updated: {}, Deleted: {}",
                comparison.newFiles.size(), comparison.updatedFiles.size(), comparison.deletedFiles.size()
            );

            // Task 6.4: Sync new and updated files
            int filesCreated = syncNewFiles(tenantId, namespace, comparison.newFiles);
            int filesUpdated = syncUpdatedFiles(tenantId, namespace, comparison.updatedFiles);
            resultBuilder.filesCreated(filesCreated);
            resultBuilder.filesUpdated(filesUpdated);

            // Task 6.5: Delete removed files
            int filesDeleted = deleteRemovedFiles(tenantId, namespace, comparison.deletedFiles);
            resultBuilder.filesDeleted(filesDeleted);

            // Task 6.5: Track total number of files processed
            int filesProcessed = comparison.newFiles.size() + comparison.updatedFiles.size() + comparison.deletedFiles.size();
            resultBuilder.filesProcessed(filesProcessed);
            log.info(
                "Processed {} files during full sync for namespace: {}/{}",
                filesProcessed, tenantId, namespace
            );

            // Task 6.6: Clean up empty directories
            cleanupAllEmptyDirectories(tenantId, namespace);

            // Task 6.7: Return SyncResult with statistics
            long duration = System.currentTimeMillis() - startTime;
            resultBuilder.totalDurationMs(duration);
            resultBuilder.success(true);

            SyncResult result = resultBuilder.build();
            log.info(
                "Full sync completed for namespace: {}/{} in {}ms - Created: {}, Updated: {}, Deleted: {}, Processed: {}",
                tenantId, namespace, duration, filesCreated, filesUpdated, filesDeleted, filesProcessed
            );

            return result;

        } catch (IOException e) {
            long duration = System.currentTimeMillis() - startTime;
            resultBuilder.totalDurationMs(duration);
            resultBuilder.success(false);
            resultBuilder.errors(
                List.of(
                    SyncError.builder()
                        .filePath("")
                        .operation("FULL_SYNC")
                        .errorMessage(e.getMessage())
                        .timestamp(Instant.now())
                        .build()
                )
            );

            log.error("Full sync failed for namespace: {}/{}", tenantId, namespace, e);
            throw new SyncException(
                "Full sync failed",
                tenantId,
                namespace,
                "",
                null,
                e
            );
        }
    }

    /**
     * Cleans up the _rill_project directory.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @throws IOException if cleanup operation fails
     * @throws SyncException if cleanup operation fails
     */
    public void cleanup(String tenantId, String namespace) throws IOException, SyncException {
        if (!config.enabled()) {
            log.debug("Sync feature is disabled, skipping cleanup");
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("Starting cleanup of _rill_project for namespace: {}/{}", tenantId, namespace);

        URI rillProjectUri = buildRillProjectUri(tenantId, namespace);

        // Delete all files under _rill_project
        try {
            List<URI> filesToDelete = storage.allByPrefix(tenantId, namespace, rillProjectUri, true);
            for (URI fileUri : filesToDelete) {
                try {
                    storage.delete(tenantId, namespace, fileUri);
                } catch (IOException e) {
                    log.debug("Failed to delete file during cleanup: {}", fileUri, e);
                }
            }
        } catch (IOException e) {
            log.debug("_rill_project directory not found during cleanup: {}/{}", tenantId, namespace);
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("Cleanup completed for namespace: {}/{} in {}ms", tenantId, namespace, duration);
    }

    // Helper methods

    /**
     * Atomically puts a file using temp file + rename pattern.
     * 
     * This method ensures atomic file operations by:
     * 1. Writing to a temporary file with a unique name
     * 2. Atomically renaming the temp file to the final destination
     * 3. Cleaning up the temp file if the operation fails
     * 
     * This pattern prevents partial writes and ensures consistency even if the
     * process crashes during the write operation.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param targetUri the target file URI
     * @param content the file content
     * @throws IOException if the atomic operation fails
     */
    private void atomicPutFile(String tenantId, String namespace, URI targetUri, InputStream content) throws IOException {
        // Generate a unique temporary file name
        String tempFileName = targetUri.toString() + ".tmp." + UUID.randomUUID();
        URI tempUri = URI.create(tempFileName);

        try {
            // Write to temporary file
            log.debug("Writing to temporary file: {}", tempUri);
            storage.put(tenantId, namespace, tempUri, content);

            // Atomically rename temp file to final destination
            log.debug("Atomically renaming {} to {}", tempUri, targetUri);
            storage.move(tenantId, namespace, tempUri, targetUri);

            log.debug("Successfully completed atomic file operation for: {}", targetUri);

        } catch (IOException e) {
            // Clean up temporary file if it exists
            try {
                log.debug("Cleaning up temporary file after failure: {}", tempUri);
                storage.delete(tenantId, namespace, tempUri);
            } catch (IOException cleanupError) {
                log.warn("Failed to clean up temporary file: {}", tempUri, cleanupError);
                // Don't throw - the original error is more important
            }

            // Throw the original error
            throw e;
        }
    }

    // Helper methods

    /**
     * Updates metadata after a successful sync operation.
     * 
     * Ensures that the metadata reflects the sync status by:
     * 1. Finding existing metadata for the file
     * 2. Updating the synced flag and lastSyncedAt timestamp
     * 3. Saving the updated metadata
     * 
     * If metadata doesn't exist, it will be created with the synced flag set to true.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path
     */
    private void updateMetadataAfterSync(String tenantId, String namespace, String filePath) {
        if (!metadataRepository.isPresent()) {
            log.debug("Metadata repository not available, skipping metadata update");
            return;
        }

        try {
            // Find the metadata for this file
            Optional<NamespaceFileMetadata> metadata = metadataRepository.get().findByPath(tenantId, namespace, filePath);

            NamespaceFileMetadata syncedMetadata;
            if (metadata.isPresent()) {
                // Update existing metadata with current timestamp
                syncedMetadata = metadata.get().toBuilder()
                    .updated(Instant.now())
                    .build();
                log.debug("Updating existing metadata for synced file: {}/{}/{}", tenantId, namespace, filePath);
            } else {
                // Create new metadata if it doesn't exist
                syncedMetadata = NamespaceFileMetadata.builder()
                    .tenantId(tenantId)
                    .namespace(namespace)
                    .path(filePath)
                    .version(1)
                    .size(0L)
                    .build();
                log.debug("Creating new metadata for synced file: {}/{}/{}", tenantId, namespace, filePath);
            }

            // Save the metadata
            metadataRepository.get().save(syncedMetadata);
            log.debug("Successfully updated metadata for synced file: {}/{}/{}", tenantId, namespace, filePath);

        } catch (IOException e) {
            log.warn("Failed to update metadata after sync for file: {}/{}/{}", tenantId, namespace, filePath, e);
            // Don't throw exception - metadata update failure should not fail the sync operation
        }
    }

    /**
     * Builds the URI for the _rill_project directory.
     */
    private URI buildRillProjectUri(String tenantId, String namespace) {
        return URI.create("kestra://" + tenantId + "/" + namespace.replace(".", "/") + "/" + RILL_PROJECT_DIR + "/");
    }

    /**
     * Builds the URI for a file in the _rill_project directory.
     */
    private URI buildRillProjectFilePath(String tenantId, String namespace, String filePath) {
        String cleanPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
        return URI.create("kestra://" + tenantId + "/" + namespace.replace(".", "/") + "/" + RILL_PROJECT_DIR + "/" + cleanPath);
    }

    /**
     * Ensures the _rill_project directory exists.
     */
    private void ensureRillProjectDirectory(String tenantId, String namespace) throws IOException {
        URI rillProjectUri = buildRillProjectUri(tenantId, namespace);
        if (!storage.exists(tenantId, namespace, rillProjectUri)) {
            log.info("Creating _rill_project directory for namespace: {}/{}", tenantId, namespace);
            storage.createDirectory(tenantId, namespace, rillProjectUri);
        }
    }

    /**
     * Creates parent directories for a file path.
     */
    private void createParentDirectories(String tenantId, String namespace, String filePath) throws IOException {
        String cleanPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
        String[] parts = cleanPath.split("/");

        StringBuilder currentPath = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            currentPath.append(parts[i]).append("/");
            URI dirUri = URI.create("kestra://" + tenantId + "/" + namespace.replace(".", "/") + "/" + RILL_PROJECT_DIR + "/" + currentPath);

            if (!storage.exists(tenantId, namespace, dirUri)) {
                storage.createDirectory(tenantId, namespace, dirUri);
            }
        }
    }

    /**
     * Identifies empty parent directories for a given file path.
     * 
     * This method analyzes the directory structure and identifies which parent directories
     * are empty after a file deletion. It works backwards from the file's parent directory
     * to the root of the _rill_project directory.
     * 
     * The method stops identifying empty directories when it encounters a non-empty directory,
     * as all parent directories above that point will also be non-empty.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path that was deleted
     * @return list of URIs representing empty parent directories, ordered from deepest to shallowest
     * @throws IOException if directory listing fails
     */
    private List<URI> identifyEmptyParentDirectories(String tenantId, String namespace, String filePath) throws IOException {
        List<URI> emptyDirectories = new ArrayList<>();

        String cleanPath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
        String[] parts = cleanPath.split("/");

        // Start from the parent directory and work backwards
        for (int i = parts.length - 2; i >= 0; i--) {
            StringBuilder currentPath = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                currentPath.append(parts[j]).append("/");
            }

            URI dirUri = URI.create("kestra://" + tenantId + "/" + namespace.replace(".", "/") + "/" + RILL_PROJECT_DIR + "/" + currentPath);

            try {
                List<FileAttributes> contents = storage.list(tenantId, namespace, dirUri);
                if (contents.isEmpty()) {
                    // Directory is empty, add it to the list
                    emptyDirectories.add(dirUri);
                    log.debug("Identified empty parent directory: {}", dirUri);
                } else {
                    // Directory is not empty, stop identifying
                    log.debug("Found non-empty directory: {}, stopping identification", dirUri);
                    break;
                }
            } catch (IOException e) {
                // Directory might not exist, stop identification
                log.debug("Directory not found or error listing contents: {}", dirUri, e);
                break;
            }
        }

        return emptyDirectories;
    }

    /**
     * Cleans up empty parent directories.
     * 
     * This method identifies empty parent directories and removes them recursively.
     * It stops when it encounters a non-empty directory.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param filePath the file path that was deleted
     * @throws IOException if cleanup operation fails
     */
    private void cleanupEmptyDirectories(String tenantId, String namespace, String filePath) throws IOException {
        try {
            // Identify empty parent directories
            List<URI> emptyDirectories = identifyEmptyParentDirectories(tenantId, namespace, filePath);

            // Remove empty directories recursively (from deepest to shallowest)
            for (URI dirUri : emptyDirectories) {
                try {
                    storage.delete(tenantId, namespace, dirUri);
                    log.debug("Deleted empty parent directory: {}", dirUri);
                } catch (IOException e) {
                    log.debug("Failed to delete empty parent directory: {}", dirUri, e);
                    // Continue with other directories
                }
            }

            if (!emptyDirectories.isEmpty()) {
                log.debug(
                    "Successfully cleaned up {} empty parent directories for file: {}",
                    emptyDirectories.size(), filePath
                );
            }

        } catch (IOException e) {
            log.warn(
                "Failed to cleanup empty parent directories for file: {}/{}/{}",
                tenantId, namespace, filePath, e
            );
            // Don't throw exception - cleanup failure should not fail the sync operation
        }
    }

    /**
     * Validates a file path to prevent directory traversal attacks and ensure it's within the namespace.
     * 
     * This method performs the following validations:
     * 1. Normalizes the file path using Paths.get().normalize()
     * 2. Checks for path traversal attempts (../)
     * 3. Validates that the path is within the namespace directory
     * 4. Rejects absolute paths
     * 
     * @param filePath the file path to validate
     * @param namespace the namespace name
     * @throws SyncException if the path is invalid or contains traversal attempts
     */
    private void validatePath(String filePath, String namespace) throws SyncException {
        if (filePath == null || filePath.isEmpty()) {
            throw new SyncException("File path cannot be null or empty", null, namespace, filePath, null, null);
        }

        // Normalize the path using Paths.get().normalize()
        java.nio.file.Path normalizedPath = Paths.get(filePath).normalize();
        String normalizedPathStr = normalizedPath.toString();

        // Check for path traversal attempts (..)
        if (normalizedPathStr.contains("..")) {
            log.error("Path traversal attempt detected: {} in namespace {}", filePath, namespace);
            throw new SyncException(
                "Path traversal attempt detected: path contains '..'",
                null,
                namespace,
                filePath,
                null,
                null
            );
        }

        // Reject absolute paths
        if (normalizedPath.isAbsolute()) {
            log.error("Absolute path rejected: {} in namespace {}", filePath, namespace);
            throw new SyncException(
                "Absolute paths are not allowed",
                null,
                namespace,
                filePath,
                null,
                null
            );
        }

        // Validate that the path is within the namespace directory
        // The path should not escape the namespace
        if (normalizedPathStr.startsWith("..") || normalizedPathStr.startsWith("/")) {
            log.error("Path outside namespace detected: {} in namespace {}", filePath, namespace);
            throw new SyncException(
                "Path is outside the namespace directory",
                null,
                namespace,
                filePath,
                null,
                null
            );
        }
    }

    /**
     * Validates both source and destination paths to prevent directory traversal attacks.
     * 
     * This method validates bidirectional paths by:
     * 1. Validating the source path
     * 2. Validating the destination path
     * 3. Ensuring both paths are within the namespace
     * 
     * @param sourcePath the source file path
     * @param destinationPath the destination file path
     * @param namespace the namespace name
     * @throws SyncException if either path is invalid
     */
    private void validateBidirectionalPaths(String sourcePath, String destinationPath, String namespace) throws SyncException {
        // Validate source path
        validatePath(sourcePath, namespace);

        // Validate destination path
        validatePath(destinationPath, namespace);

        log.debug(
            "Bidirectional path validation passed for source: {} and destination: {} in namespace: {}",
            sourcePath, destinationPath, namespace
        );
    }

    /**
     * Records the start of a sync operation.
     */
    private void recordSyncStart(SyncOperation operation) {
        if (stateManager.isPresent()) {
            try {
                stateManager.get().recordSyncStart(operation);
            } catch (IOException e) {
                log.warn("Failed to record sync start: {}", operation.getId(), e);
            }
        }
    }

    /**
     * Records the success of a sync operation.
     */
    private void recordSyncSuccess(SyncOperation operation, long duration) {
        if (stateManager.isPresent()) {
            try {
                stateManager.get().recordSyncSuccess(operation.getId(), duration);
            } catch (IOException e) {
                log.warn("Failed to record sync success: {}", operation.getId(), e);
            }
        }

        if (metricsCollector.isPresent()) {
            metricsCollector.get().recordOperation(
                operation.getTenantId(),
                operation.getNamespace(),
                operation,
                duration,
                true
            );
        }
    }

    /**
     * Records the failure of a sync operation.
     */
    private void recordSyncFailure(SyncOperation operation, IOException error, long duration) {
        if (stateManager.isPresent()) {
            try {
                stateManager.get().recordSyncFailure(operation.getId(), error);
            } catch (IOException e) {
                log.warn("Failed to record sync failure: {}", operation.getId(), e);
            }
        }

        if (metricsCollector.isPresent()) {
            metricsCollector.get().recordOperation(
                operation.getTenantId(),
                operation.getNamespace(),
                operation,
                duration,
                false
            );
        }
    }

    // Full Sync Helper Methods

    /**
     * Task 6.1: Get list of files in latest version.
     * 
     * This method retrieves all files from the latest version of the namespace.
     * It scans the _files directory to find the latest version and lists all files in it.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @return list of FileInfo objects representing files in the latest version
     * @throws IOException if file listing fails
     */
    private List<FileInfo> getLatestVersionFiles(String tenantId, String namespace) throws IOException {
        List<FileInfo> files = new ArrayList<>();

        try {
            // Get the latest version directory
            URI filesBaseUri = URI.create(namespace + "/_files/");
            List<FileAttributes> versions = storage.list(tenantId, namespace, filesBaseUri);

            // Find the latest version (highest numeric version)
            int latestVersion = 0;
            for (FileAttributes attr : versions) {
                try {
                    String fileName = attr.getFileName();
                    String versionStr = fileName.replaceAll("[^0-9]", "");
                    if (!versionStr.isEmpty()) {
                        int version = Integer.parseInt(versionStr);
                        if (version > latestVersion) {
                            latestVersion = version;
                        }
                    }
                } catch (NumberFormatException e) {
                    // Skip non-numeric directories
                    log.debug("Skipping non-numeric version directory: {}", attr.getFileName());
                }
            }

            if (latestVersion > 0) {
                // List all files in the latest version
                URI latestVersionUri = URI.create(namespace + "/_files/v" + latestVersion + "/");
                files = listFilesRecursively(tenantId, namespace, latestVersionUri);
                log.debug(
                    "Found {} files in latest version (v{}) for namespace: {}/{}",
                    files.size(), latestVersion, tenantId, namespace
                );
            } else {
                log.debug("No versioned files found for namespace: {}/{}", tenantId, namespace);
            }

        } catch (IOException e) {
            log.warn("Failed to get latest version files for namespace: {}/{}", tenantId, namespace, e);
            // Return empty list if no files found
        }

        return files;
    }

    /**
     * Task 6.2: Get list of files in current _rill_project.
     * 
     * This method retrieves all files currently in the _rill_project directory.
     * It recursively lists all files and directories in _rill_project.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @return list of FileInfo objects representing files in _rill_project
     * @throws IOException if file listing fails
     */
    private List<FileInfo> getRillProjectFiles(String tenantId, String namespace) throws IOException {
        List<FileInfo> files = new ArrayList<>();

        try {
            URI rillProjectUri = buildRillProjectUri(tenantId, namespace);

            // Check if _rill_project directory exists
            if (storage.exists(tenantId, namespace, rillProjectUri)) {
                files = listFilesRecursively(tenantId, namespace, rillProjectUri);
                log.debug(
                    "Found {} files in _rill_project for namespace: {}/{}",
                    files.size(), tenantId, namespace
                );
            } else {
                log.debug("_rill_project directory does not exist for namespace: {}/{}", tenantId, namespace);
            }

        } catch (IOException e) {
            log.warn("Failed to get _rill_project files for namespace: {}/{}", tenantId, namespace, e);
            // Return empty list if directory doesn't exist
        }

        return files;
    }

    /**
     * Task 6.3: Compare files and identify changes.
     * 
     * This method compares the files in the latest version with the files in _rill_project
     * and identifies which files are new, updated, or deleted.
     * 
     * @param latestVersionFiles files from the latest version
     * @param rillProjectFiles files from _rill_project
     * @return FileComparison object containing new, updated, and deleted files
     */
    private FileComparison compareFiles(List<FileInfo> latestVersionFiles, List<FileInfo> rillProjectFiles) {
        FileComparison comparison = new FileComparison();

        // Create maps for quick lookup
        java.util.Map<String, FileInfo> latestMap = new java.util.HashMap<>();
        java.util.Map<String, FileInfo> rillMap = new java.util.HashMap<>();

        for (FileInfo file : latestVersionFiles) {
            latestMap.put(file.path, file);
        }

        for (FileInfo file : rillProjectFiles) {
            rillMap.put(file.path, file);
        }

        // Find new and updated files
        for (FileInfo latestFile : latestVersionFiles) {
            if (!rillMap.containsKey(latestFile.path)) {
                // File is new
                comparison.newFiles.add(latestFile);
                log.debug("Identified new file: {}", latestFile.path);
            } else {
                // File exists, check if it needs updating
                FileInfo rillFile = rillMap.get(latestFile.path);
                if (!latestFile.checksum.equals(rillFile.checksum)) {
                    // File content has changed
                    comparison.updatedFiles.add(latestFile);
                    log.debug("Identified updated file: {}", latestFile.path);
                }
            }
        }

        // Find deleted files
        for (FileInfo rillFile : rillProjectFiles) {
            if (!latestMap.containsKey(rillFile.path)) {
                // File no longer exists in latest version
                comparison.deletedFiles.add(rillFile);
                log.debug("Identified deleted file: {}", rillFile.path);
            }
        }

        return comparison;
    }

    /**
     * Task 6.4: Sync new and updated files.
     * 
     * This method syncs new files and updated files to _rill_project.
     * It copies the file content from the latest version to _rill_project.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param newFiles list of new files to sync
     * @return number of files synced
     */
    private int syncNewFiles(String tenantId, String namespace, List<FileInfo> newFiles) {
        int count = 0;

        for (FileInfo file : newFiles) {
            try {
                log.debug("Syncing new file: {}", file.path);

                // Create parent directories if needed
                createParentDirectories(tenantId, namespace, file.path);

                // Copy file from latest version to _rill_project
                URI sourceUri = file.sourceUri;
                URI destUri = buildRillProjectFilePath(tenantId, namespace, file.path);

                // Read from source and write to destination
                InputStream content = storage.get(tenantId, namespace, sourceUri);
                atomicPutFile(tenantId, namespace, destUri, content);

                count++;
                log.debug("Successfully synced new file: {}", file.path);

            } catch (IOException e) {
                log.error("Failed to sync new file: {}", file.path, e);
                // Continue with other files
            }
        }

        return count;
    }

    /**
     * Task 6.4 (continued): Sync updated files.
     * 
     * This method syncs updated files to _rill_project.
     * It replaces the existing file content with the new content from the latest version.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param updatedFiles list of updated files to sync
     * @return number of files synced
     */
    private int syncUpdatedFiles(String tenantId, String namespace, List<FileInfo> updatedFiles) {
        int count = 0;

        for (FileInfo file : updatedFiles) {
            try {
                log.debug("Syncing updated file: {}", file.path);

                // Copy file from latest version to _rill_project (overwrite)
                URI sourceUri = file.sourceUri;
                URI destUri = buildRillProjectFilePath(tenantId, namespace, file.path);

                // Read from source and write to destination
                InputStream content = storage.get(tenantId, namespace, sourceUri);
                atomicPutFile(tenantId, namespace, destUri, content);

                count++;
                log.debug("Successfully synced updated file: {}", file.path);

            } catch (IOException e) {
                log.error("Failed to sync updated file: {}", file.path, e);
                // Continue with other files
            }
        }

        return count;
    }

    /**
     * Task 6.5: Delete removed files.
     * 
     * This method deletes files from _rill_project that no longer exist in the latest version.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param deletedFiles list of files to delete
     * @return number of files deleted
     */
    private int deleteRemovedFiles(String tenantId, String namespace, List<FileInfo> deletedFiles) {
        int count = 0;

        for (FileInfo file : deletedFiles) {
            try {
                log.debug("Deleting removed file: {}", file.path);

                URI destUri = buildRillProjectFilePath(tenantId, namespace, file.path);
                storage.delete(tenantId, namespace, destUri);

                count++;
                log.debug("Successfully deleted removed file: {}", file.path);

            } catch (IOException e) {
                log.error("Failed to delete removed file: {}", file.path, e);
                // Continue with other files
            }
        }

        return count;
    }

    /**
     * Task 6.6: Clean up empty directories.
     * 
     * This method removes all empty parent directories from _rill_project.
     * It starts from the deepest directories and works backwards.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     */
    private void cleanupAllEmptyDirectories(String tenantId, String namespace) {
        try {
            URI rillProjectUri = buildRillProjectUri(tenantId, namespace);

            // Get all directories in _rill_project
            List<URI> allItems = storage.allByPrefix(tenantId, namespace, rillProjectUri, true);

            // Sort by path depth (deepest first) to clean up from bottom to top
            java.util.List<URI> directories = new java.util.ArrayList<>();
            for (URI itemUri : allItems) {
                try {
                    FileAttributes attr = storage.getAttributes(tenantId, namespace, itemUri);
                    if (attr.getType() == FileAttributes.FileType.Directory) {
                        directories.add(itemUri);
                    }
                } catch (IOException e) {
                    log.debug("Failed to get attributes for item: {}", itemUri, e);
                }
            }

            // Sort by depth (deepest first)
            directories.sort((a, b) ->
            {
                int depthA = a.toString().split("/").length;
                int depthB = b.toString().split("/").length;
                return Integer.compare(depthB, depthA);
            });

            // Try to delete each directory
            for (URI dirUri : directories) {
                try {
                    List<FileAttributes> contents = storage.list(tenantId, namespace, dirUri);
                    if (contents.isEmpty()) {
                        storage.delete(tenantId, namespace, dirUri);
                        log.debug("Deleted empty directory: {}", dirUri);
                    }
                } catch (IOException e) {
                    // Directory might not be empty or might not exist, skip it
                    log.debug("Could not delete directory: {}", dirUri, e);
                }
            }

        } catch (IOException e) {
            log.warn("Failed to cleanup empty directories for namespace: {}/{}", tenantId, namespace, e);
            // Don't throw exception - cleanup failure should not fail the sync operation
        }
    }

    /**
     * Lists all files recursively in a directory.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param dirUri the directory URI
     * @return list of FileInfo objects for all files in the directory
     * @throws IOException if file listing fails
     */
    private List<FileInfo> listFilesRecursively(String tenantId, String namespace, URI dirUri) throws IOException {
        List<FileInfo> files = new ArrayList<>();

        try {
            List<URI> allFiles = storage.allByPrefix(tenantId, namespace, dirUri, true);

            for (URI fileUri : allFiles) {
                try {
                    FileAttributes attr = storage.getAttributes(tenantId, namespace, fileUri);

                    if (attr.getType() == FileAttributes.FileType.File) {
                        // Extract the relative path from the URI
                        String fullPath = fileUri.toString();
                        String relativePath = fullPath.replace(namespace + "/_rill_project/", "")
                            .replace(namespace + "/_files/v\\d+/", "");

                        // Calculate checksum for comparison
                        String checksum = calculateFileChecksum(tenantId, namespace, fileUri);

                        files.add(new FileInfo(relativePath, fileUri, checksum));
                    }
                } catch (IOException e) {
                    log.debug("Failed to get attributes for file: {}", fileUri, e);
                }
            }

        } catch (IOException e) {
            log.warn("Failed to list files recursively in directory: {}", dirUri, e);
            throw e;
        }

        return files;
    }

    /**
     * Calculates a checksum for a file to detect changes.
     * 
     * @param tenantId the tenant ID
     * @param namespace the namespace name
     * @param fileUri the file URI
     * @return the checksum of the file
     * @throws IOException if file reading fails
     */
    private String calculateFileChecksum(String tenantId, String namespace, URI fileUri) throws IOException {
        try {
            InputStream content = storage.get(tenantId, namespace, fileUri);
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = content.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }
            content.close();

            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();

        } catch (Exception e) {
            log.warn("Failed to calculate checksum for file: {}", fileUri, e);
            // Return empty string if checksum calculation fails
            return "";
        }
    }

    /**
     * Helper class to represent file information for comparison.
     */
    private static class FileInfo {
        String path;
        URI sourceUri;
        String checksum;

        FileInfo(String path, URI sourceUri, String checksum) {
            this.path = path;
            this.sourceUri = sourceUri;
            this.checksum = checksum;
        }
    }

    /**
     * Helper class to represent file comparison results.
     */
    private static class FileComparison {
        List<FileInfo> newFiles = new ArrayList<>();
        List<FileInfo> updatedFiles = new ArrayList<>();
        List<FileInfo> deletedFiles = new ArrayList<>();
    }
}
