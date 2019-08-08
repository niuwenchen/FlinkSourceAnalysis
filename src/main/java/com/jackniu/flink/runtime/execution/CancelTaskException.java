package com.jackniu.flink.runtime.execution;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class CancelTaskException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CancelTaskException(Throwable cause) {
        super(cause);
    }

    public CancelTaskException(String msg) {
        super(msg);
    }

    public CancelTaskException() {
        super();
    }
}

