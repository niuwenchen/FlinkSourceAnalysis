package com.jackniu.flink.api.java.typeutils;

/**
 * Created by JackNiu on 2019/6/24.
 */
public class TypeExtractionException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with no message.
     */
    public TypeExtractionException() {
        super();
    }

    /**
     * Creates a new exception with the given message.
     *
     * @param message The exception message.
     */
    public TypeExtractionException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with the given message and cause.
     *
     * @param message The exception message.
     * @param e cause
     */
    public TypeExtractionException(String message, Throwable e) {
        super(message, e);
    }

}
