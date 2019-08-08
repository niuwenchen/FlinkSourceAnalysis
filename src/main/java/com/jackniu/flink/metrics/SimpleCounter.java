package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class SimpleCounter implements Counter {

    /** the current count. */
    private long count;

    /**
     * Increment the current count by 1.
     */
    @Override
    public void inc() {
        count++;
    }

    /**
     * Increment the current count by the given value.
     *
     * @param n value to increment the current count by
     */
    @Override
    public void inc(long n) {
        count += n;
    }

    /**
     * Decrement the current count by 1.
     */
    @Override
    public void dec() {
        count--;
    }

    /**
     * Decrement the current count by the given value.
     *
     * @param n value to decrement the current count by
     */
    @Override
    public void dec(long n) {
        count -= n;
    }

    /**
     * Returns the current count.
     *
     * @return current count
     */
    @Override
    public long getCount() {
        return count;
    }
}
