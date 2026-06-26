package io.kestra.core.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Service for executing flows synchronously in the current thread, bypassing queues and worker threads.
 * This is used for webhook triggers with `wait: true` to achieve low latency responses.
 */
@Singleton
public class FastExecutionService {

    @Inject
    private RunContextFactory runContextFactory;

    /**
     * Execute a flow synchronously in the current thread.
     */
    public Execution executeFlow(Flow flow, Execution execution) {
        if (flow == null || execution == null) {
            return execution.withState(State.Type.FAILED);
        }

        try {
            List<Task> tasks = flow.getTasks();
            if (tasks == null || tasks.isEmpty()) {
                Execution result = execution.withState(State.Type.SUCCESS).withOutputs(new HashMap<>());
                return evaluateFlowOutputs(flow, result, runContextFactory);
            }

            Map<String, Object> allOutputs = new HashMap<>();
            List<TaskRun> taskRuns = new ArrayList<>();
            Execution currentExecution = execution;

            for (Task task : tasks) {
                TaskRun taskRun = TaskRun.builder()
                    .id(io.kestra.core.utils.IdUtils.create())
                    .tenantId(execution.getTenantId())
                    .namespace(execution.getNamespace())
                    .flowId(execution.getFlowId())
                    .executionId(execution.getId())
                    .taskId(task.getId())
                    .state(new State().withState(State.Type.RUNNING))
                    .build();

                try {
                    RunContext runContext = runContextFactory.of(flow, task, currentExecution, taskRun);

                    if (task instanceof RunnableTask<?> runnableTask) {
                        Object output = runnableTask.run(runContext);

                        Map<String, Object> taskOutputMap = new HashMap<>();
                        if (output != null) {
                            if (output instanceof io.kestra.core.models.tasks.Output taskOutput) {
                                taskOutputMap.put(task.getId(), taskOutput.toMap());
                            } else {
                                taskOutputMap.put(task.getId(), output);
                            }
                        }

                        allOutputs.putAll(taskOutputMap);
                        taskRuns.add(taskRun.withState(State.Type.SUCCESS));

                    } else {
                        taskRuns.add(taskRun.withState(State.Type.SKIPPED));
                    }

                    currentExecution = currentExecution
                        .withTaskRunList(new ArrayList<>(taskRuns))
                        .withOutputs(new HashMap<>(allOutputs));

                    runContext.cleanup();

                } catch (Exception e) {
                    taskRuns.add(taskRun.withState(State.Type.FAILED));
                    currentExecution = currentExecution.withTaskRunList(new ArrayList<>(taskRuns));
                    return currentExecution.withState(State.Type.FAILED);
                }
            }

            Execution result = currentExecution.withState(State.Type.SUCCESS);
            return evaluateFlowOutputs(flow, result, runContextFactory);

        } catch (Exception e) {
            return execution.withState(State.Type.FAILED);
        }
    }

    private Execution evaluateFlowOutputs(Flow flow, Execution execution, RunContextFactory runContextFactory) {
        if (flow.getOutputs() == null || flow.getOutputs().isEmpty()) {
            return execution;
        }

        try {
            RunContext runContext = runContextFactory.of(flow, execution);
            Map<String, Object> outputsMap = execution.getOutputs() != null ? new HashMap<>(execution.getOutputs()) : new HashMap<>();
            Map<String, Object> evaluatedOutputs = new HashMap<>();

            for (io.kestra.core.models.flows.Output output : flow.getOutputs()) {
                try {
                    String valueStr = output.getValue() != null ? output.getValue().toString() : "";
                    Object renderedValue = runContext.render(valueStr, Map.of("outputs", outputsMap));

                    if (output.getType() != null && "JSON".equalsIgnoreCase(output.getType().toString()) && renderedValue instanceof String jsonStr) {
                        try {
                            renderedValue = io.kestra.core.serializers.JacksonMapper.toMap(jsonStr);
                        } catch (Exception ignored) {
                            // Keep as string if JSON parsing fails
                        }
                    }

                    evaluatedOutputs.put(output.getId(), renderedValue);
                } catch (Exception ignored) {
                    // Skip output if evaluation fails
                }
            }

            return execution.withOutputs(evaluatedOutputs);
        } catch (Exception e) {
            return execution;
        }
    }
}
