package com.jackniu.flink.runtime.checkpoint;

import java.io.Serializable;

import static com.jackniu.flink.util.Preconditions.checkArgument;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class SubtaskStateStats implements Serializable {

    private static final long serialVersionUID = 8928594531621862214L;

    /** Index of this sub task. */
    private final int subtaskIndex;

    /**
     * Timestamp when the ack from this sub task was received at the
     * coordinator.
     */
    private final long ackTimestamp;

    /** Size of the checkpointed state at this subtask. */
    private final long stateSize;

    /** Checkpoint duration at the operator (sync part) in milliseconds. */
    private final long syncCheckpointDuration;

    /** Checkpoint duration at the operator (async part) in milliseconds. */
    private final long asyncCheckpointDuration;

    /** Number of buffered bytes during alignment. */
    private final long alignmentBuffered;

    /** Alignment duration in . */
    private final long alignmentDuration;

    /**
     * Creates the stats for a single subtask.
     *
     * @param subtaskIndex Index of the subtask.
     * @param ackTimestamp Timestamp when the acknowledgement of this subtask was received at the coordinator.
     * @param stateSize Size of the checkpointed state at this subtask.
     * @param syncCheckpointDuration Checkpoint duration at the task (synchronous part)
     * @param asyncCheckpointDuration  Checkpoint duration at the task (asynchronous part)
     * @param alignmentBuffered Bytes buffered during stream alignment (for exactly-once only).
     * @param alignmentDuration Duration of the stream alignment (for exactly-once only).
     */
    SubtaskStateStats(
            int subtaskIndex,
            long ackTimestamp,
            long stateSize,
            long syncCheckpointDuration,
            long asyncCheckpointDuration,
            long alignmentBuffered,
            long alignmentDuration) {

        checkArgument(subtaskIndex >= 0, "Negative subtask index");
        this.subtaskIndex = subtaskIndex;
        checkArgument(stateSize >= 0, "Negative state size");
        this.stateSize = stateSize;
        this.ackTimestamp = ackTimestamp;
        this.syncCheckpointDuration = syncCheckpointDuration;
        this.asyncCheckpointDuration = asyncCheckpointDuration;
        this.alignmentBuffered = alignmentBuffered;
        this.alignmentDuration = alignmentDuration;
    }

    /**
     * Returns the subtask index.
     *
     * @return Subtask index.
     */
    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    /**
     * Returns the size of the checkpointed state at this subtask.
     *
     * @return Checkpoint state size of the sub task.
     */
    public long getStateSize() {
        return stateSize;
    }

    /**
     * Returns the timestamp when the acknowledgement of this subtask was
     * received at the coordinator.
     *
     * @return ACK timestamp at the coordinator.
     */
    public long getAckTimestamp() {
        return ackTimestamp;
    }

    /**
     * Computes the duration since the given trigger timestamp.
     *
     * <p>If the trigger timestamp is greater than the ACK timestamp, this
     * returns <code>0</code>.
     *
     * @param triggerTimestamp Trigger timestamp of the checkpoint.
     * @return Duration since the given trigger timestamp.
     */
    public long getEndToEndDuration(long triggerTimestamp) {
        return Math.max(0, ackTimestamp - triggerTimestamp);
    }

    /**
     * Returns the duration of the synchronous part of the checkpoint.
     *
     * <p>Can return <code>-1</code> if the runtime did not report this.
     *
     * @return Duration of the synchronous part of the checkpoint or <code>-1</code>.
     */
    public long getSyncCheckpointDuration() {
        return syncCheckpointDuration;
    }

    /**
     * Returns the duration of the asynchronous part of the checkpoint.
     *
     * <p>Can return <code>-1</code> if the runtime did not report this.
     *
     * @return Duration of the asynchronous part of the checkpoint or <code>-1</code>.
     */
    public long getAsyncCheckpointDuration() {
        return asyncCheckpointDuration;
    }

    /**
     * Returns the number of bytes buffered during stream alignment (for
     * exactly-once only).
     *
     * <p>Can return <code>-1</code> if the runtime did not report this.
     *
     * @return Number of bytes buffered during stream alignment or <code>-1</code>.
     */
    public long getAlignmentBuffered() {
        return alignmentBuffered;
    }

    /**
     * Returns the duration of the stream alignment (for exactly-once only).
     *
     * <p>Can return <code>-1</code> if the runtime did not report this.
     *
     * @return Duration of the stream alignment or <code>-1</code>.
     */
    public long getAlignmentDuration() {
        return alignmentDuration;
    }

}

