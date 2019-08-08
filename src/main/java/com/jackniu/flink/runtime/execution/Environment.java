package com.jackniu.flink.runtime.execution;

import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.JobID;
import com.jackniu.flink.api.common.TaskInfo;
import com.jackniu.flink.configuration.Configuration;
import com.jackniu.flink.core.fs.Path;
import com.jackniu.flink.runtime.accumulators.AccumulatorRegistry;
import com.jackniu.flink.runtime.broadcast.BroadcastVariableManager;
import com.jackniu.flink.runtime.checkpoint.CheckpointMetrics;
import com.jackniu.flink.runtime.executiongraph.ExecutionAttemptID;
import com.jackniu.flink.runtime.io.disk.iomanager.IOManager;
import com.jackniu.flink.runtime.io.network.TaskEventDispatcher;
import com.jackniu.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import com.jackniu.flink.runtime.io.network.partition.consumer.InputGate;
import com.jackniu.flink.runtime.jobgraph.JobVertexID;
import com.jackniu.flink.runtime.memory.MemoryManager;
import com.jackniu.flink.runtime.metrics.groups.TaskMetricGroup;
import com.jackniu.flink.runtime.query.TaskKvStateRegistry;
import com.jackniu.flink.runtime.state.TaskStateManager;
import com.jackniu.flink.runtime.taskmanager.TaskManagerRuntimeInfo;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * Created by JackNiu on 2019/7/6.
 */
/**
 * The Environment gives the code executed in a task access to the task's properties
 * (such as name, parallelism), the configurations, the data stream readers and writers,
 * as well as the various components that are provided by the TaskManager, such as
 * memory manager, I/O manager, ...
 */

public interface Environment {
    ExecutionConfig getExecutionConfig();
    JobID getJobID();
    JobVertexID getJobVertexId();
    /**
     * Gets the ID of the task execution attempt.
     *
     * @return The ID of the task execution attempt.
     */
    ExecutionAttemptID getExecutionId();
    Configuration getTaskConfiguration();
    TaskManagerRuntimeInfo getTaskManagerInfo();

    /**
     * Returns the task specific metric group.
     *
     * @return The MetricGroup of this task.
     */
    TaskMetricGroup getMetricGroup();

    /**
     * Returns the job-wide configuration object that was attached to the JobGraph.
     *
     * @return The job-wide configuration
     */
    Configuration getJobConfiguration();

    /**
     * Returns the {@link TaskInfo} object associated with this subtask
     *
     * @return TaskInfo for this subtask
     */
    TaskInfo getTaskInfo();

    /**
     * Returns the input split provider assigned to this environment.
     *
     * @return The input split provider or {@code null} if no such
     *         provider has been assigned to this environment.
     */
    InputSplitProvider getInputSplitProvider();

    /**
     * Returns the current {@link IOManager}.
     *
     * @return the current {@link IOManager}.
     */
    IOManager getIOManager();

    /**
     * Returns the current {@link MemoryManager}.
     *
     * @return the current {@link MemoryManager}.
     */
    MemoryManager getMemoryManager();

    /**
     * Returns the user code class loader
     */
    ClassLoader getUserClassLoader();

    Map<String, Future<Path>> getDistributedCacheEntries();

    BroadcastVariableManager getBroadcastVariableManager();

    TaskStateManager getTaskStateManager();

    /**
     * Return the registry for accumulators which are periodically sent to the job manager.
     * @return the registry
     */
    AccumulatorRegistry getAccumulatorRegistry();

    /**
     * Returns the registry for {@link } instances.
     *
     * @return KvState registry
     */
    TaskKvStateRegistry getTaskKvStateRegistry();

    /**
     * Confirms that the invokable has successfully completed all steps it needed to
     * to for the checkpoint with the give checkpoint-ID. This method does not include
     * any state in the checkpoint.
     *
     * @param checkpointId ID of this checkpoint
     * @param checkpointMetrics metrics for this checkpoint
     */
    void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics);

    /**
     * Confirms that the invokable has successfully completed all required steps for
     * the checkpoint with the give checkpoint-ID. This method does include
     * the given state in the checkpoint.
     *
     * @param checkpointId ID of this checkpoint
     * @param checkpointMetrics metrics for this checkpoint
     * @param subtaskState All state handles for the checkpointed state
     */
    void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState);

    /**
     * Declines a checkpoint. This tells the checkpoint coordinator that this task will
     * not be able to successfully complete a certain checkpoint.
     *
     * @param checkpointId The ID of the declined checkpoint.
     * @param cause An optional reason why the checkpoint was declined.
     */
    void declineCheckpoint(long checkpointId, Throwable cause);

    /**
     * Marks task execution failed for an external reason (a reason other than the task code itself
     * throwing an exception). If the task is already in a terminal state
     * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
     * Otherwise it sets the state to FAILED, and, if the invokable code is running,
     * starts an asynchronous thread that aborts that code.
     *
     * <p>This method never blocks.
     */
    void failExternally(Throwable cause);

    // --------------------------------------------------------------------------------------------
    //  Fields relevant to the I/O system. Should go into Task
    // --------------------------------------------------------------------------------------------

    ResultPartitionWriter getWriter(int index);

    ResultPartitionWriter[] getAllWriters();

    InputGate getInputGate(int index);

    InputGate[] getAllInputGates();

    TaskEventDispatcher getTaskEventDispatcher();
}
