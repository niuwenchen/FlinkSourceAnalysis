package com.jackniu.flink.streaming.api.environment;

import com.jackniu.flink.streaming.api.CheckpointingMode;

import java.io.Serializable;

import static com.jackniu.flink.util.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

/**
 * Created by JackNiu on 2019/6/17.
 */
public class CheckpointConfig   implements Serializable {
    private static final long serialVersionUID = -750378776078908147L;

    /** The default checkpoint mode: exactly once. */
    public static final CheckpointingMode DEFAULT_MODE = CheckpointingMode.EXACTLY_ONCE;

    /** The default timeout of a checkpoint attempt: 10 minutes. 默认10分钟 */
    public static final long DEFAULT_TIMEOUT = 10 * 60 * 1000;

    /** The default minimum pause to be made between checkpoints: none. */
    public static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 0;

    /** The default limit of concurrently happening checkpoints: one. */
    public static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;

    // ------------------------------------------------------------------------

    /** Checkpointing mode (exactly-once vs. at-least-once). */
    private CheckpointingMode checkpointingMode = DEFAULT_MODE;

    /** Periodic 周期性的 checkpoint triggering interval. */
    private long checkpointInterval = -1; // disabled

    /** Maximum time checkpoint may take before being discarded. */
    private long checkpointTimeout = DEFAULT_TIMEOUT;

    /** Minimal pause between checkpointing attempts. */
    private long minPauseBetweenCheckpoints = DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS;

    /** Maximum number of checkpoint attempts in progress at the same time. */
    private int maxConcurrentCheckpoints = DEFAULT_MAX_CONCURRENT_CHECKPOINTS;

    /** Flag to force checkpointing in iterative jobs. */
    private boolean forceCheckpointing;

    /** Cleanup behaviour for persistent checkpoints. */
    private ExternalizedCheckpointCleanup externalizedCheckpointCleanup;

    /** Determines if a tasks are failed or not if there is an error in their checkpointing. Default: true */
    private boolean failOnCheckpointingErrors = true;


    // 如果 checkpointInterval(-1) 大于0 则检查
    public boolean isCheckpointingEnabled() {
        return checkpointInterval > 0;

    }
    public CheckpointingMode getCheckpointingMode() {
        return checkpointingMode;
    }
    //Sets the checkpointing mode (exactly-once vs. at-least-once).
    public void setCheckpointingMode(CheckpointingMode checkpointingMode) {
        this.checkpointingMode = requireNonNull(checkpointingMode);
    }
    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        if (checkpointInterval <= 0) {
            throw new IllegalArgumentException("Checkpoint interval must be larger than zero");
        }
        this.checkpointInterval = checkpointInterval;
    }
    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    //Sets the maximum time that a checkpoint may take before being discarded.
    public void setCheckpointTimeout(long checkpointTimeout) {
        if (checkpointTimeout <= 0) {
            throw new IllegalArgumentException("Checkpoint timeout must be larger than zero");
        }
        this.checkpointTimeout = checkpointTimeout;
    }

    public long getMinPauseBetweenCheckpoints() {
        return minPauseBetweenCheckpoints;
    }

    public void setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
        if (minPauseBetweenCheckpoints < 0) {
            throw new IllegalArgumentException("Pause value must be zero or positive");
        }
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
    }

    public int getMaxConcurrentCheckpoints() {
        return maxConcurrentCheckpoints;
    }

    public void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
        if (maxConcurrentCheckpoints < 1) {
            throw new IllegalArgumentException("The maximum number of concurrent attempts must be at least one.");
        }
        this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
    }
    public boolean isForceCheckpointing() {
        return forceCheckpointing;
    }
    public void setForceCheckpointing(boolean forceCheckpointing) {
        this.forceCheckpointing = forceCheckpointing;
    }
    public boolean isFailOnCheckpointingErrors() {
        return failOnCheckpointingErrors;
    }
    public void setFailOnCheckpointingErrors(boolean failOnCheckpointingErrors) {
        this.failOnCheckpointingErrors = failOnCheckpointingErrors;
    }
    public void enableExternalizedCheckpoints(ExternalizedCheckpointCleanup cleanupMode) {
        this.externalizedCheckpointCleanup = checkNotNull(cleanupMode);
    }

    public boolean isExternalizedCheckpointsEnabled() {
        return externalizedCheckpointCleanup != null;
    }
    public ExternalizedCheckpointCleanup getExternalizedCheckpointCleanup() {
        return externalizedCheckpointCleanup;
    }


    /**
     * 当job被取消之后 清除行为
     */
    public enum ExternalizedCheckpointCleanup {
        // Delete externalized checkpoints on job cancellation.
        DELETE_ON_CANCELLATION(true),
        RETAIN_ON_CANCELLATION(false);

        private final boolean deleteOnCancellation;

        ExternalizedCheckpointCleanup(boolean deleteOnCancellation) {
            this.deleteOnCancellation = deleteOnCancellation;
        }

        public boolean deleteOnCancellation() {
            return deleteOnCancellation;
        }
    }

}
