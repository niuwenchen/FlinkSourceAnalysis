package com.jackniu.flink.runtime.operators.chaining;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class ExceptionInChainedStubException extends RuntimeException {

    private static final long serialVersionUID = -7966910518892776903L;

    private String taskName;

    private Exception exception;

    public ExceptionInChainedStubException(String taskName, Exception wrappedException) {
        super("Exception in chained task '" + taskName + "'", exceptionUnwrap(wrappedException));
        this.taskName = taskName;
        this.exception = wrappedException;
    }

    public String getTaskName() {
        return taskName;
    }

    public Exception getWrappedException() {
        return exception;
    }

    public static Exception exceptionUnwrap(Exception e) {
        if (e instanceof ExceptionInChainedStubException) {
            return exceptionUnwrap(((ExceptionInChainedStubException) e).getWrappedException());
        }

        return e;
    }
}
