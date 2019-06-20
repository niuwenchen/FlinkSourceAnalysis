package com.jackniu.flink.api.common.functions;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class InvalidProgramException  extends RuntimeException{
    private static final long serialVersionUID = 3119881934024032887L;

    /**
     * Creates a new exception with no message.
     */
    public InvalidProgramException() {
        super();
    }

    /**
     * Creates a new exception with the given message.
     *
     * @param message The exception message.
     */
    public InvalidProgramException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with the given message and cause.
     *
     * @param message The exception message.
     * @param e The exception cause.
     */
    public InvalidProgramException(String message, Throwable e) {
        super(message, e);
    }
}
