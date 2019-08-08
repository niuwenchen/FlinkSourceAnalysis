package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public abstract class HistogramStatistics {
    /**
     * Returns the value for the given quantile based on the represented histogram statistics.
     *
     * @param quantile Quantile to calculate the value for
     * @return Value for the given quantile
     */
    public abstract double getQuantile(double quantile);

    /**
     * Returns the elements of the statistics' sample.
     *
     * @return Elements of the statistics' sample
     */
    public abstract long[] getValues();

    /**
     * Returns the size of the statistics' sample.
     *
     * @return Size of the statistics' sample
     */
    public abstract int size();

    /**
     * Returns the mean of the histogram values.
     *
     * @return Mean of the histogram values
     */
    public abstract double getMean();

    /**
     * Returns the standard deviation of the distribution reflected by the histogram statistics.
     *
     * @return Standard deviation of histogram distribution
     */
    public abstract double getStdDev();

    /**
     * Returns the maximum value of the histogram.
     *
     * @return Maximum value of the histogram
     */
    public abstract long getMax();

    /**
     * Returns the minimum value of the histogram.
     *
     * @return Minimum value of the histogram
     */
    public abstract long getMin();
}
