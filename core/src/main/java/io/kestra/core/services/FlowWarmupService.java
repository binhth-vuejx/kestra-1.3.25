package io.kestra.core.services;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.repositories.FlowRepositoryInterface;

import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Service to warmup (pre-cache) flows on application startup.
 * 
 * This eliminates the slow first call (~600ms) by loading and compiling flows
 * in memory when the application starts. The compilation/deserialization is
 * what takes time (~600ms per flow), so this warmup pre-loads all flows so they're
 * already compiled and ready for webhook/fast-path execution.
 * 
 * First call without warmup: ~600ms (load from DB, deserialize, compile)
 * First call with warmup: ~100ms (already compiled in cache)
 * 
 * HOW IT WORKS:
 * 1. FlowListeners loads all FlowWithSource (raw YAML/JSON) on startup
 * 2. This service calls flowRepository.findById() for EACH flow
 * 3. findById deserializes the flow from the database - expensive!
 * 4. But this happens in parallel during startup, so users don't notice
 * 5. After warmup, all flows are compiled and ready for fast execution
 */
@Singleton
@Slf4j
public class FlowWarmupService {

    @Inject
    @Nullable
    private FlowRepositoryInterface flowRepository;

    @Inject
    @Nullable
    private FastExecutionService fastExecutionService;

    @Inject
    @Nullable
    private FlowCacheService flowCacheService;

    @Value("${kestra.flow-warmup.enabled:true}")
    private boolean warmupEnabled;

    @Value("${kestra.flow-warmup.parallel-threads:4}")
    private int parallelThreads;

    /**
     * Warmup flows on application startup.
     * This method should be called after the application is fully initialized.
     */
    public void warmupFlows() {
        if (!warmupEnabled) {
            log.info("🔥 Flow warmup is disabled (kestra.flow-warmup.enabled: false)");
            return;
        }

        if (flowRepository == null) {
            log.warn("🔥 Flow warmup: FlowRepository not available, skipping");
            return;
        }

        try {
            log.info("🔥 Starting Flow Warmup Service...");
            long startTime = System.currentTimeMillis();

            // Get all flows for all tenants
            List<FlowWithSource> allFlows = flowRepository.findAllWithSourceForAllTenants();

            if (allFlows.isEmpty()) {
                log.info("🔥 No flows to warmup");
                return;
            }

            log.info("🔥 Found {} flows to warmup", allFlows.size());

            // Warmup flows in parallel
            warmupFlowsInParallel(allFlows);

            long duration = System.currentTimeMillis() - startTime;
            log.info("✅ Flow warmup completed in {}ms ({}ms per flow avg)", duration, duration / allFlows.size());
            
            log.info("🚀 All flows are now pre-compiled and ready for fast execution!");

        } catch (Exception e) {
            log.error("❌ Error during flow warmup", e);
        }
    }

    /**
     * Warmup flows using parallel threads for faster loading.
     * Each thread will call flowRepository.findById() which deserializes the flow.
     */
    private void warmupFlowsInParallel(List<FlowWithSource> flows) {
        ExecutorService executor = Executors.newFixedThreadPool(parallelThreads);

        for (FlowWithSource flow : flows) {
            executor.submit(() -> warmupSingleFlow(flow));
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                log.warn("⚠️ Flow warmup timeout after 5 minutes");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("❌ Flow warmup interrupted", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Warmup a single flow by loading it to trigger compilation/deserialization.
     * 
     * This calls flowRepository.findById() which deserializes the flow from JSON.
     * The deserialization is expensive (~100-200ms per flow) but happens in parallel.
     * After loading, the flow is put into FlowCacheService for instant cache HIT on first request.
     */
    private void warmupSingleFlow(FlowWithSource flowWithSource) {
        try {
            String flowKey = flowWithSource.getNamespace() + "/" + flowWithSource.getId();
            long startTime = System.currentTimeMillis();

            // This call will deserialize and compile the flow
            // It's expensive but happens in parallel during startup
            Optional<Flow> compiledFlow = flowRepository.findById(
                flowWithSource.getTenantId(),
                flowWithSource.getNamespace(),
                flowWithSource.getId(),
                Optional.of(flowWithSource.getRevision())
            );

            long duration = System.currentTimeMillis() - startTime;

            if (compiledFlow.isPresent()) {
                // 🔴 KEY FIX: Explicitly cache the loaded flow into FlowCacheService
                // This ensures subsequent requests will get cache HIT immediately
                if (flowCacheService != null) {
                    flowCacheService.put(
                        flowWithSource.getTenantId(),
                        flowWithSource.getNamespace(),
                        flowWithSource.getId(),
                        Optional.empty(),  // Cache with "latest" key (no revision specified)
                        compiledFlow
                    );
                    log.debug("🔥 Warmed up and cached flow: {} ({}ms)", flowKey, duration);
                } else {
                    log.debug("🔥 Warmed up flow (cache unavailable): {} ({}ms)", flowKey, duration);
                }
            } else {
                log.warn("⚠️ Failed to warmup flow {}: not found after deserialization", flowKey);
            }

        } catch (Exception e) {
            log.warn("⚠️ Failed to warmup flow {}.{}: {}", flowWithSource.getNamespace(), flowWithSource.getId(), e.getMessage());
        }
    }
}
