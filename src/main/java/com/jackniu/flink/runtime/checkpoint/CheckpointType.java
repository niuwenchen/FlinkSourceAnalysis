package com.jackniu.flink.runtime.checkpoint;

/**
 * Created by JackNiu on 2019/7/6.
 */
public enum CheckpointType {
    /** A checkpoint, full or incremental. */
    CHECKPOINT,

    /** A savepoint. */
    SAVEPOINT;

}
