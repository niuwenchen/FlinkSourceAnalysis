package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/6/20.
 */
public interface Collector<T> {
    void collect(T record);

    /**
     * Closes the collector. If any data was buffered, that data will be flushed.
     */
    void close();
}
