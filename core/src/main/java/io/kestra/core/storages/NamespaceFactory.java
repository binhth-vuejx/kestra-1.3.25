package io.kestra.core.storages;

import org.slf4j.Logger;

import io.kestra.core.repositories.NamespaceFileMetadataRepositoryInterface;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;

@Singleton
public class NamespaceFactory {
    @Inject
    private NamespaceFileMetadataRepositoryInterface namespaceFileMetadataRepositoryInterface;

    @Inject
    private Optional<io.kestra.core.namespace.RillProjectSyncService> rillProjectSyncService = Optional.empty();

    public Namespace of(String tenantId, String namespace, StorageInterface storageInterface) {
        return new InternalNamespace(tenantId, namespace, storageInterface, namespaceFileMetadataRepositoryInterface, rillProjectSyncService);
    }

    public Namespace of(Logger logger, String tenantId, String namespace, StorageInterface storageInterface) {
        return new InternalNamespace(logger, tenantId, namespace, storageInterface, namespaceFileMetadataRepositoryInterface, rillProjectSyncService);
    }
}
