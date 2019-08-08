package com.jackniu.flink.runtime.operators.util;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class CorruptConfigurationException  extends RuntimeException
{
    private static final long serialVersionUID = 854450995262666207L;

    /**
     * Creates a new exception with the given error message.
     *
     * @param message The exception's message.
     */
    public CorruptConfigurationException(String message) {
        super(message);
    }

    public CorruptConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}

