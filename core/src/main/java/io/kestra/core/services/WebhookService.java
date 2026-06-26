package io.kestra.core.services;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.net.URIBuilder;

import io.kestra.core.events.CrudEvent;
import io.kestra.core.models.Label;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.FlowInputOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.trace.propagation.ExecutionTextMapSetter;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.UriProvider;
import io.kestra.plugin.core.trigger.AbstractWebhookTrigger;
import io.kestra.plugin.core.trigger.WebhookContext;
import io.kestra.plugin.core.trigger.WebhookResponse;

import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.sse.Event;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import static io.kestra.core.models.Label.CORRELATION_ID;

@Slf4j
@Singleton
public class WebhookService {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ConditionService conditionService;

    @Inject
    private FlowInputOutput flowInputOutput;

    @Inject
    private ExecutionStreamingService streamingService;

    @Inject
    private UriProvider uriProvider;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    private ApplicationEventPublisher<CrudEvent<Execution>> eventPublisher;

    @Inject
    private Optional<OpenTelemetry> openTelemetry;

    @Inject
    private Optional<FastExecutionService> fastExecutionService = Optional.empty();

    /**
     * Parse query parameters from the webhook request URI.
     *
     * @param context The webhook context containing the request
     * @return A map of query parameter names to their list of values
     */
    public Map<String, List<String>> parseParameters(WebhookContext context) {
        URIBuilder uriBuilder = new URIBuilder(context.request().getUri());

        return uriBuilder.getQueryParams()
            .stream()
            .collect(
                Collectors.groupingBy(
                    NameValuePair::getName,
                    Collectors.mapping(NameValuePair::getValue, Collectors.toList())
                )
            );
    }

    /**
     * Prepare the execution checking conditions, and injecting inputs.
     *
     * @param context The webhook context containing request, path, flow, and services
     * @param trigger The webhook trigger
     * @param output The trigger output to attach to the execution
     * @return The prepared execution, or empty if conditions are not met
     */
    public Optional<Execution> newExecution(WebhookContext context, Flow flow, AbstractWebhookTrigger trigger, io.kestra.core.models.tasks.Output output) {
        Execution execution = Execution.builder()
            .id(IdUtils.create())
            .tenantId(context.flow().getTenantId())
            .namespace(context.flow().getNamespace())
            .flowId(context.flow().getId())
            .flowRevision(context.flow().getRevision())
            .inputs(trigger.getInputs())
            .variables(context.flow().getVariables())
            .state(new State())
            .trigger(ExecutionTrigger.of(trigger, output))
            .build();

        // Add labels
        List<Label> labels = new ArrayList<>();
        labels.add(new Label(Label.FROM, "trigger"));
        labels.addAll(LabelService.labelsExcludingSystem(flow.getLabels()));
        if (labels.stream().noneMatch(label -> label.key().equals(CORRELATION_ID))) {
            labels.add(new Label(CORRELATION_ID, execution.getId()));
        }

        execution = execution.withLabels(labels);

        // Check conditions
        var runContext = runContext(flow, execution);

        var conditionContext = conditionService.conditionContext(runContext, flow, execution, null);
        if (trigger.getConditions() != null && !conditionService.valid(flow, trigger.getConditions(), conditionContext)) {
            return Optional.empty(); // Conditions not met
        }

        // Inject trigger inputs
        if (trigger.getInputs() != null) {
            try {
                Map<String, Object> renderedInputs = runContext.render(trigger.getInputs());
                renderedInputs = readExecutionInputs(flow, execution, renderedInputs);
                execution = execution.withInputs(renderedInputs);
            } catch (Exception e) {
                log.warn("Unable to render the webhook inputs. Webhook will be ignored", e);
                return Optional.empty(); // Input rendering failed
            }
        }

        return Optional.of(execution);
    }

    /**
     * Start the execution by injecting trace context and emitting it to the execution queue.
     *
     * @param execution The execution to start
     * @throws QueueException If there is an error emitting to the queue
     */
    public void startExecution(Execution execution) throws QueueException {
        // inject the traceparent into the execution
        Optional<TextMapPropagator> propagator = openTelemetry
            .map(OpenTelemetry::getPropagators)
            .map(ContextPropagators::getTextMapPropagator);

        propagator.ifPresent(
            textMapPropagator -> textMapPropagator.inject(
                Context.current(),
                execution,
                ExecutionTextMapSetter.INSTANCE
            )
        );

        executionQueue.emit(execution);
        eventPublisher.publishEvent(CrudEvent.create(execution));
    }

    /**
     * Follow the execution events as a Flux stream.
     *
     * @param execution The execution to follow
     * @param flow The flow associated with the execution
     * @return A Flux stream of execution events
     */
    public Flux<Event<Execution>> followExecution(Execution execution, Flow flow) {
        var subscriberId = UUID.randomUUID().toString();
        var executionId = execution.getId();

        return Flux.<Event<Execution>> create(emitter ->
        {
            streamingService.registerSubscriber(
                executionId,
                subscriberId,
                emitter,
                flow
            );
        })
            .doFinally(signalType -> streamingService.unregisterSubscriber(executionId, subscriberId));
    }

    /**
     * Create a WebhookResponse from the execution.
     *
     * @param execution The execution to create the response from
     * @return The WebhookResponse
     */
    public WebhookResponse executionResponse(Execution execution) {
        return WebhookResponse.fromExecution(
            execution,
            uriProvider.executionUrl(execution)
        );
    }

    /**
     * Create a RunContext for the given flow and trigger.
     *
     * @param flow The flow
     * @param trigger The trigger
     * @return The RunContext
     */
    public RunContext runContext(Flow flow, AbstractTrigger trigger) {
        return runContextFactory.of(flow, trigger);
    }

    /**
     * Create a RunContext for the given flow and execution.
     *
     * @param flow The flow
     * @param execution The execution
     * @return The RunContext
     */
    public RunContext runContext(Flow flow, Execution execution) {
        return runContextFactory.of(flow, execution);
    }

    /**
     * Read and process execution inputs using FlowInputOutput service.
     *
     * @param flow The flow
     * @param execution The execution
     * @param renderedInputs The rendered inputs
     * @return The processed execution inputs
     */
    public Map<String, Object> readExecutionInputs(Flow flow, Execution execution, Map<String, Object> renderedInputs) {
        return flowInputOutput.readExecutionInputs(flow, execution, renderedInputs);
    }

    /**
     * Generate the webhook URL for the given flow and trigger.
     *
     * @param flow The flow
     * @param trigger The webhook trigger
     * @return The webhook URL
     */
    public URI url(Flow flow, AbstractWebhookTrigger trigger) {
        return uriProvider.webhookUrl(flow, trigger);
    }

    /**
     * Check if the webhook trigger should use the fast path for execution.
     * Fast path is used when the trigger has wait:true to bypass queues and execute synchronously.
     *
     * @param trigger The webhook trigger
     * @return true if fast path should be used, false otherwise
     */
    public boolean shouldUseFastPath(AbstractWebhookTrigger trigger) {
        // Check if trigger is a Webhook instance with wait property
        if (trigger instanceof io.kestra.plugin.core.trigger.Webhook webhook) {
            boolean useFastPath = Boolean.TRUE.equals(webhook.getWait());
            log.info(
                "🚀 WEBHOOK FAST PATH CHECK: trigger={}, wait={}, useFastPath={}, fastExecutionService.available={}",
                webhook.getId(), webhook.getWait(), useFastPath, !fastExecutionService.isEmpty()
            );

            if (!useFastPath) {
                log.info("⚠️ WEBHOOK FAST PATH DISABLED: wait is {} (expected: true)", webhook.getWait());
            }
            return useFastPath && !fastExecutionService.isEmpty();
        }
        log.info("⚠️ WEBHOOK FAST PATH CHECK: trigger is not Webhook instance, type={}", trigger.getClass().getSimpleName());
        return false;
    }

    /**
     * Execute a flow using the fast path (synchronously) and return the result.
     * This bypasses the execution queue and worker queue for low-latency responses.
     *
     * @param flow The flow to execute
     * @param execution The execution to run
     * @return A Mono containing the completed execution
     */
    public reactor.core.publisher.Mono<Execution> executeFlowFastPath(Flow flow, Execution execution) {
        if (fastExecutionService.isEmpty()) {
            log.warn("❌ FastExecutionService not available, falling back to normal path");
            return reactor.core.publisher.Mono.empty();
        }

        try {
            long startTime = System.currentTimeMillis();
            log.info(
                "⚡ FAST PATH EXECUTION STARTED: flow={}/{}, executionId={}, startTime={}",
                flow.getNamespace(), flow.getId(), execution.getId(), startTime
            );

            // Execute the flow synchronously
            Execution completedExecution = fastExecutionService.get().executeFlow(flow, execution);

            long duration = System.currentTimeMillis() - startTime;
            log.info(
                "✅ FAST PATH EXECUTION COMPLETED: flow={}/{}, executionId={}, state={}, duration={}ms",
                flow.getNamespace(), flow.getId(), execution.getId(), completedExecution.getState().getCurrent(), duration
            );

            if (duration > 1000) {
                log.warn("⚠️ FAST PATH SLOW: Execution took {}ms (expected <100ms)", duration);
            }

            // Persist the completed execution to database
            try {
                persistFastExecution(completedExecution);
                log.info(
                    "✅ FAST PATH PERSISTENCE: Execution persisted successfully for flow={}/{}, executionId={}",
                    flow.getNamespace(), flow.getId(), execution.getId()
                );
            } catch (Exception persistError) {
                log.error(
                    "❌ FAST PATH PERSISTENCE ERROR: Failed to persist execution for flow={}/{}, executionId={}: {}",
                    flow.getNamespace(), flow.getId(), execution.getId(), persistError.getMessage(), persistError
                );
            }

            return reactor.core.publisher.Mono.just(completedExecution);
        } catch (Exception e) {
            log.error(
                "❌ FAST PATH EXECUTION ERROR: flow={}/{}, executionId={}, error={}",
                flow.getNamespace(), flow.getId(), execution.getId(), e.getMessage(), e
            );
            // Return execution with FAILED state
            Execution failedExecution = execution.withState(State.Type.FAILED);
            return reactor.core.publisher.Mono.just(failedExecution);
        }
    }

    /**
     * Persist a completed fast-path execution to the database.
     * This ensures the execution is visible in the UI and stored permanently.
     *
     * @param execution The completed execution to persist
     * @throws QueueException If there is an error emitting to the queue
     */
    public void persistFastExecution(Execution execution) throws QueueException {
        // Emit execution to queue for persistence
        executionQueue.emit(execution);

        // Publish event for UI updates
        eventPublisher.publishEvent(CrudEvent.create(execution));

        log.info(
            "📝 FAST PATH PERSISTENCE: Execution persisted - executionId={}, state={}, outputs={}",
            execution.getId(), execution.getState().getCurrent(), execution.getOutputs()
        );
    }
}