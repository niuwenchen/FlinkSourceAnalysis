package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;
import java.util.Map;

import static com.jackniu.flink.util.Preconditions.checkArgument;
import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class CompletedCheckpointStats extends AbstractCheckpointStats {

    private static final long serialVersionUID = 138833868551861344L;

    /** Total checkpoint state size over all subtasks. */
    private final long stateSize;

    /** Buffered bytes during alignment over all subtasks. */
    private final long alignmentBuffered;

    /** The latest acknowledged subtask stats. */
    private final SubtaskStateStats latestAcknowledgedSubtask;

    /** The external pointer of the checkpoint. */
    private final String externalPointer;

    /** Flag indicating whether the checkpoint was discarded. */
    private volatile boolean discarded;

    /**
     * Creates a tracker for a {@link CompletedCheckpoint}.
     *
     * @param checkpointId ID of the checkpoint.
     * @param triggerTimestamp Timestamp when the checkpoint was triggered.
     * @param props Checkpoint properties of the checkpoint.
     * @param totalSubtaskCount Total number of subtasks for the checkpoint.
     * @param taskStats Task stats for each involved operator.
     * @param numAcknowledgedSubtasks Number of acknowledged subtasks.
     * @param stateSize Total checkpoint state size over all subtasks.
     * @param alignmentBuffered Buffered bytes during alignment over all subtasks.
     * @param latestAcknowledgedSubtask The latest acknowledged subtask stats.
     * @param externalPointer Optional external path if persisted externally.
     */
    CompletedCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            int numAcknowledgedSubtasks,
            long stateSize,
            long alignmentBuffered,
            SubtaskStateStats latestAcknowledgedSubtask,
            String externalPointer) {

        super(checkpointId, triggerTimestamp, props, totalSubtaskCount, taskStats);
        checkArgument(numAcknowledgedSubtasks == totalSubtaskCount, "Did not acknowledge all subtasks.");
        checkArgument(stateSize >= 0, "Negative state size");
        this.stateSize = stateSize;
        this.alignmentBuffered = alignmentBuffered;
        this.latestAcknowledgedSubtask = checkNotNull(latestAcknowledgedSubtask);
        this.externalPointer = externalPointer;
    }

    @Override
    public CheckpointStatsStatus getStatus() {
        return CheckpointStatsStatus.COMPLETED;
    }

    @Override
    public int getNumberOfAcknowledgedSubtasks() {
        return numberOfSubtasks;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    @Override
    public long getAlignmentBuffered() {
        return alignmentBuffered;
    }

    @Override
    @Nullable
    public SubtaskStateStats getLatestAcknowledgedSubtaskStats() {
        return latestAcknowledgedSubtask;
    }

    // ------------------------------------------------------------------------
    // Completed checkpoint specific methods
    // ------------------------------------------------------------------------

    /**
     * Returns the external pointer of this checkpoint.
     */
    public String getExternalPath() {
        return externalPointer;
    }

    /**
     * Returns whether the checkpoint has been discarded.
     *
     * @return <code>true</code> if the checkpoint has been discarded, <code>false</code> otherwise.
     */
    public boolean isDiscarded() {
        return discarded;
    }

    /**
     * Returns the callback for the {@link CompletedCheckpoint}.
     *
     * @return Callback for the {@link CompletedCheckpoint}.
     */
    DiscardCallback getDiscardCallback() {
        return new DiscardCallback();
    }

    /**
     * Callback for the {@link CompletedCheckpoint} instance to notify about
     * disposal of the checkpoint (most commonly when the checkpoint has been
     * subsumed by a newer one).
     */
    class DiscardCallback {

        /**
         * Updates the discarded flag of the checkpoint stats.
         *
         * <p>After this notification, {@link #isDiscarded()} will return
         * <code>true</code>.
         */
        void notifyDiscardedCheckpoint() {
            discarded = true;
        }

    }

    @Override
    public String toString() {
        return "CompletedCheckpoint(id=" + getCheckpointId() + ")";
    }
}

