package com.jackniu.flink.runtime.checkpoint;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class MinMaxAvgStats implements Serializable {

    private static final long serialVersionUID = 1769601903483446707L;

    /** Current min value. */
    private long min;

    /** Current max value. */
    private long max;

    /** Sum of all added values. */
    private long sum;

    /** Count of added values. */
    private long count;

    MinMaxAvgStats() {
    }

    private MinMaxAvgStats(long min, long max, long sum, long count) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
    }

    /**
     * Adds the value to the stats if it is >= 0.
     *
     * @param value Value to add for min/max/avg stats..
     */
    void add(long value) {
        if (value >= 0) {
            if (count > 0) {
                min =  Math.min(min, value);
                max = Math.max(max, value);
            } else {
                min = value;
                max = value;
            }

            count++;
            sum += value;
        }
    }

    /**
     * Returns a snapshot of the current state.
     *
     * @return A snapshot of the current state.
     */
    MinMaxAvgStats createSnapshot() {
        return new MinMaxAvgStats(min, max, sum, count);
    }

    /**
     * Returns the minimum seen value.
     *
     * @return The current minimum value.
     */
    public long getMinimum() {
        return min;
    }

    /**
     * Returns the maximum seen value.
     *
     * @return The current maximum value.
     */
    public long getMaximum() {
        return max;
    }

    /**
     * Returns the sum of all seen values.
     *
     * @return Sum of all values.
     */
    public long getSum() {
        return sum;
    }

    /**
     * Returns the count of all seen values.
     *
     * @return Count of all values.
     */
    public long getCount() {
        return count;
    }

    /**
     * Calculates the average over all seen values.
     *
     * @return Average over all seen values.
     */
    public long getAverage() {
        if (count == 0) {
            return 0;
        } else {
            return sum / count;
        }
    }

}

