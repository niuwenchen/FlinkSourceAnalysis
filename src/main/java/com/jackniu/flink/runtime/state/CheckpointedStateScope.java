package com.jackniu.flink.runtime.state;

/**
 * Created by JackNiu on 2019/7/5.
 */
public enum CheckpointedStateScope {
    /**
     * Exclusive state belongs exclusively to one specific checkpoint / savepoint.
     */
    EXCLUSIVE,

    /**
     * Shared state may belong to more than one checkpoint.
     *
     * <p>Shared state is typically used for incremental or differential checkpointing
     * methods, where only deltas are written, and state from prior checkpoints is
     * referenced in newer checkpoints as well.
     */
    SHARED

}
