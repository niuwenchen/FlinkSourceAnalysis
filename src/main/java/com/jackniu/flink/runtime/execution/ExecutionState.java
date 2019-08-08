package com.jackniu.flink.runtime.execution;

/**
 * Created by JackNiu on 2019/7/7.
 */
public enum ExecutionState {
    CREATED,

    SCHEDULED,

    DEPLOYING,

    RUNNING,

    /**
     * This state marks "successfully completed". It can only be reached when a
     * program reaches the "end of its input". The "end of input" can be reached
     * when consuming a bounded input (fix set of files, bounded query, etc) or
     * when stopping a program (not cancelling!) which make the input look like
     * it reached its end at a specific point.
     */
    FINISHED,

    CANCELING,

    CANCELED,

    FAILED,

    RECONCILING;

    public boolean isTerminal() {
        return this == FINISHED || this == CANCELED || this == FAILED;
    }
}
