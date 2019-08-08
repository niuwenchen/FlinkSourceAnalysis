package com.jackniu.flink.runtime.state;

import com.jackniu.flink.runtime.checkpoint.CheckpointMetaData;
import com.jackniu.flink.runtime.checkpoint.CheckpointMetrics;
import com.jackniu.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import com.jackniu.flink.runtime.checkpoint.TaskStateSnapshot;
import com.jackniu.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Created by JackNiu on 2019/7/8.
 */
public interface TaskStateManager extends CheckpointListener {

    /**
     * Report the state snapshots for the operator instances running in the owning task.
     *
     * @param checkpointMetaData meta data from the checkpoint request.
     * @param checkpointMetrics  task level metrics for the checkpoint.
     * @param acknowledgedState  the reported states to acknowledge to the job manager.
     * @param localState         the reported states for local recovery.
     */
    void reportTaskStateSnapshots(
            @Nonnull CheckpointMetaData checkpointMetaData,
            @Nonnull CheckpointMetrics checkpointMetrics,
            @Nullable TaskStateSnapshot acknowledgedState,
            @Nullable TaskStateSnapshot localState);

    /**
     * Returns means to restore previously reported state of an operator running in the owning task.
     *
     * @param operatorID the id of the operator for which we request state.
     * @return Previous state for the operator. The previous state can be empty if the operator had no previous state.
     */
    @Nonnull
    PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID);

    /**
     * Returns the configuration for local recovery, i.e. the base directories for all file-based local state of the
     * owning subtask and the general mode for local recovery.
     */
    @Nonnull
    LocalRecoveryConfig createLocalRecoveryConfig();
}

