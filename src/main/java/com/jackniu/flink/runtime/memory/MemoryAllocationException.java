package com.jackniu.flink.runtime.memory;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class MemoryAllocationException extends Exception {

    private static final long serialVersionUID = -403983866457947012L;

    public MemoryAllocationException() {
        super();
    }

    public MemoryAllocationException(String message, Throwable cause) {
        super(message, cause);
    }

    public MemoryAllocationException(String message) {
        super(message);
    }

    public MemoryAllocationException(Throwable cause) {
        super(cause);
    }
}

