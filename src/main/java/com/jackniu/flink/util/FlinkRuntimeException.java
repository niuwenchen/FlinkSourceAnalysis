package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/6/25.
 */
public class FlinkRuntimeException  extends RuntimeException{
    private static final long serialVersionUID = 193141189399279147L;

    /**
     * Creates a new Exception with the given message and null as the cause.
     *
     * @param message The exception message
     */
    public FlinkRuntimeException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with a null message and the given cause.
     *
     * @param cause The exception that caused this exception
     */
    public FlinkRuntimeException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new exception with the given message and cause.
     *
     * @param message The exception message
     * @param cause The exception that caused this exception
     */
    public FlinkRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
