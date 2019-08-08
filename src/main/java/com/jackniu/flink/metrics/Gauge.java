package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface Gauge<T> extends Metric {

    /**
     * Calculates and returns the measured value.
     *
     * @return calculated value
     */
    T getValue();
}