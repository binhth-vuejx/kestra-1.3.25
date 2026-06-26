package io.kestra.core.services;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.kestra.core.models.flows.Flow;
import io.kestra.core.repositories.FlowRepositoryInterface;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Flow cache service using Caffeine for fast flow lookups.
 * 
 * Cache key format: {tenantId}:{namespace}:{flowId}:{revision}
 * Example: main:company.team:sparrow_519079:51
 * 
 * TTL: 5 minutes (can be configured)
 */
@Singleton
@Slf4j
public class FlowCacheService {

    private final Cache<String, Optional<Flow>> cache;

    @Inject
    private FlowRepositoryInterface flowRepository;

    public FlowCacheService() {
        // Initialize Caffeine cache with 5 minute TTL and max 10000 entries
        this.cache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .maximumSize(10000)
            .recordStats()
            .build();
        
        //log.info("[FLOW_CACHE] Initialized: TTL=5m, maxSize=10000");
    }

    /**
     * Get flow from cache or load from repository if not cached.
     * 
     * @param tenantId the tenant ID
     * @param namespace the flow namespace
     * @param id the flow ID
     * @param revision the flow revision (optional)
     * @return Optional containing the flow if found
     */
    public Optional<Flow> get(String tenantId, String namespace, String id, Optional<Integer> revision) {
        String cacheKey = buildCacheKey(tenantId, namespace, id, revision);
        
        // Try to get from cache first
        Optional<Flow> cached = cache.getIfPresent(cacheKey);
        if (cached != null) {
            //log.debug("[FLOW_CACHE] ✓ Cache HIT for key: {} (result: {})", cacheKey, cached.isPresent() ? "found" : "not found");
            return cached;
        }

        // Cache miss - load from repository
        //log.debug("[FLOW_CACHE] ✗ Cache MISS for key: {}", cacheKey);
        long startTime = System.currentTimeMillis();
        
        Optional<Flow> result = flowRepository.findById(tenantId, namespace, id, revision, false);
        
        long loadTime = System.currentTimeMillis() - startTime;
        
        // Store in cache (even if not found, to avoid repeated DB queries)
        cache.put(cacheKey, result);
        
        //log.debug("[FLOW_CACHE] Loaded flow in {}ms, cached for key: {}", loadTime, cacheKey);
        
        return result;
    }

    /**
     * Put a flow directly into cache (used by warmup service).
     * Useful when the flow is already loaded and we just want to cache it.
     */
    public void put(String tenantId, String namespace, String id, Optional<Integer> revision, Optional<Flow> flow) {
        String cacheKey = buildCacheKey(tenantId, namespace, id, revision);
        cache.put(cacheKey, flow);
        //log.debug("[FLOW_CACHE] Cached flow for key: {} (result: {})", cacheKey, flow.isPresent() ? "found" : "not found");
    }

    /**
     * Invalidate cache entry for a specific flow.
     */
    public void invalidate(String tenantId, String namespace, String id) {
        // Invalidate all revisions of this flow
        cache.asMap().keySet().stream()
            .filter(key -> key.contains(buildCacheKeyPrefix(tenantId, namespace, id)))
            .forEach(cache::invalidate);
        
        //log.debug("[FLOW_CACHE] Invalidated cache for: {}:{}:{}", tenantId, namespace, id);
    }

    /**
     * Clear all cache entries.
     */
    public void clear() {
        cache.invalidateAll();
        //log.info("[FLOW_CACHE] Cleared all cache entries");
    }

    /**
     * Get cache statistics.
     */
    public void logStats() {
        //var stats = cache.stats();
        //log.info("[FLOW_CACHE] Stats - hits: {}, misses: {}, hitRate: {}", 
        //    stats.hitCount(), stats.missCount(), 
        //    String.format("%.2f%%", stats.hitRate() * 100));
    }

    private String buildCacheKey(String tenantId, String namespace, String id, Optional<Integer> revision) {
        return revision.isEmpty() 
            ? String.format("%s:%s:%s:latest", tenantId, namespace, id)
            : String.format("%s:%s:%s:%d", tenantId, namespace, id, revision.get());
    }

    private String buildCacheKeyPrefix(String tenantId, String namespace, String id) {
        return String.format("%s:%s:%s", tenantId, namespace, id);
    }
}
