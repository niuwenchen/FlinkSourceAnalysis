package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface Histogram extends Metric {

    /**
     * Update the histogram with the given value.
     *
     * @param value Value to update the histogram with
     */
    void update(long value);

    /**
     * Get the count of seen elements.
     *
     * @return Count of seen elements
     */
    long getCount();

    /**
     * Create statistics for the currently recorded elements.
     *
     * @return Statistics about the currently recorded elements
     */
    HistogramStatistics getStatistics();
}
