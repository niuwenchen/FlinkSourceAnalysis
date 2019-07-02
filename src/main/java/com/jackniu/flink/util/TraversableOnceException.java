package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/7/1.
 */
public class TraversableOnceException extends RuntimeException {

    private static final long serialVersionUID = 7636881584773577290L;

    /**
     * Creates a new exception with a default message.
     */
    public TraversableOnceException() {
        super("The Iterable can be iterated over only once. Only the first call to 'iterator()' will succeed.");
    }
}