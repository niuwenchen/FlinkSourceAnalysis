package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class FlinkException extends Exception {

    private static final long serialVersionUID = 450688772469004724L;

    /**
     * Creates a new Exception with the given message and null as the cause.
     *
     * @param message The exception message
     */
    public FlinkException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with a null message and the given cause.
     *
     * @param cause The exception that caused this exception
     */
    public FlinkException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new exception with the given message and cause.
     *
     * @param message The exception message
     * @param cause The exception that caused this exception
     */
    public FlinkException(String message, Throwable cause) {
        super(message, cause);
    }
}

