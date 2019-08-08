package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface Counter extends  Metric {
    /**
     * Increment the current count by 1.
     */
    void inc();

    /**
     * Increment the current count by the given value.
     *
     * @param n value to increment the current count by
     */
    void inc(long n);

    /**
     * Decrement the current count by 1.
     */
    void dec();

    /**
     * Decrement the current count by the given value.
     *
     * @param n value to decrement the current count by
     */
    void dec(long n);

    /**
     * Returns the current count.
     *
     * @return current count
     */
    long getCount();
}
