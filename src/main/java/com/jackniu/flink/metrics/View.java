package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface View {
    /** The interval in which metrics are updated. */
    int UPDATE_INTERVAL_SECONDS = 5;

    /**
     * This method will be called regularly to update the metric.
     */
    void update();

}
