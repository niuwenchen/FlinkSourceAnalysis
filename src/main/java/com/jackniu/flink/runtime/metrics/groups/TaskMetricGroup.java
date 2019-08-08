package com.jackniu.flink.runtime.metrics.groups;

import com.jackniu.flink.metrics.CharacterFilter;
import com.jackniu.flink.runtime.jobgraph.JobVertexID;
import com.jackniu.flink.runtime.jobgraph.OperatorID;
import com.jackniu.flink.runtime.metrics.MetricRegistry;
import com.jackniu.flink.runtime.metrics.dump.QueryScopeInfo;
import com.jackniu.flink.runtime.metrics.scope.ScopeFormat;
import com.jackniu.flink.util.AbstractID;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static com.jackniu.flink.util.Preconditions.checkNotNull;


/**
 * Created by JackNiu on 2019/7/7.
 */
public class TaskMetricGroup extends ComponentMetricGroup<TaskManagerJobMetricGroup> {

    private final Map<String, OperatorMetricGroup> operators = new HashMap<>();

    static final int METRICS_OPERATOR_NAME_MAX_LENGTH = 80;

    private final TaskIOMetricGroup ioMetrics;

    /** The execution Id uniquely identifying the executed task represented by this metrics group. */
    private final AbstractID executionId;

    @Nullable
    protected final JobVertexID vertexId;

    @Nullable
    private final String taskName;

    protected final int subtaskIndex;

    private final int attemptNumber;

    // ------------------------------------------------------------------------

    public TaskMetricGroup(
            MetricRegistry registry,
            TaskManagerJobMetricGroup parent,
            @Nullable JobVertexID vertexId,
            AbstractID executionId,
            @Nullable String taskName,
            int subtaskIndex,
            int attemptNumber) {
        super(registry, registry.getScopeFormats().getTaskFormat().formatScope(
                checkNotNull(parent), vertexId, checkNotNull(executionId), taskName, subtaskIndex, attemptNumber), parent);

        this.executionId = checkNotNull(executionId);
        this.vertexId = vertexId;
        this.taskName = taskName;
        this.subtaskIndex = subtaskIndex;
        this.attemptNumber = attemptNumber;

        this.ioMetrics = new TaskIOMetricGroup(this);
    }

    // ------------------------------------------------------------------------
    //  properties
    // ------------------------------------------------------------------------

    public final TaskManagerJobMetricGroup parent() {
        return parent;
    }

    public AbstractID executionId() {
        return executionId;
    }

    @Nullable
    public AbstractID vertexId() {
        return vertexId;
    }

    @Nullable
    public String taskName() {
        return taskName;
    }

    public int subtaskIndex() {
        return subtaskIndex;
    }

    public int attemptNumber() {
        return attemptNumber;
    }

    /**
     * Returns the TaskIOMetricGroup for this task.
     *
     * @return TaskIOMetricGroup for this task.
     */
    public TaskIOMetricGroup getIOMetricGroup() {
        return ioMetrics;
    }

    @Override
    protected QueryScopeInfo.TaskQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        return new QueryScopeInfo.TaskQueryScopeInfo(
                this.parent.jobId.toString(),
                String.valueOf(this.vertexId),
                this.subtaskIndex);
    }

    // ------------------------------------------------------------------------
    //  operators and cleanup
    // ------------------------------------------------------------------------

    public OperatorMetricGroup getOrAddOperator(String name) {
        return getOrAddOperator(OperatorID.fromJobVertexID(vertexId), name);
    }

    public OperatorMetricGroup getOrAddOperator(OperatorID operatorID, String name) {
        if (name != null && name.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
            LOG.warn("The operator name {} exceeded the {} characters length limit and was truncated.", name, METRICS_OPERATOR_NAME_MAX_LENGTH);
            name = name.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
        }
        OperatorMetricGroup operator = new OperatorMetricGroup(this.registry, this, operatorID, name);
        // unique OperatorIDs only exist in streaming, so we have to rely on the name for batch operators
        final String key = operatorID + name;

        synchronized (this) {
            OperatorMetricGroup previous = operators.put(key, operator);
            if (previous == null) {
                // no operator group so far
                return operator;
            } else {
                // already had an operator group. restore that one.
                operators.put(key, previous);
                return previous;
            }
        }
    }

    @Override
    public void close() {
        super.close();

        parent.removeTaskMetricGroup(executionId);
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_TASK_VERTEX_ID, vertexId.toString());
        variables.put(ScopeFormat.SCOPE_TASK_NAME, taskName);
        variables.put(ScopeFormat.SCOPE_TASK_ATTEMPT_ID, executionId.toString());
        variables.put(ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, String.valueOf(attemptNumber));
        variables.put(ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, String.valueOf(subtaskIndex));
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return operators.values();
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "task";
    }
}

