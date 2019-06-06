package com.jackniu.flink.streaming.api.environment;

import com.jackniu.flink.streaming.api.TimeCharacteristic;

import java.util.ArrayList;

/**
 * Created by JackNiu on 2019/6/6.
 */
public abstract class StreamExecutionEnvironment {
    public static final String DEFAULT_JOB_NAME = "Flink Streaming Job";
    private static final TimeCharacteristic DEFAULT_TIME_CHARACTERISTIC;
    private static final long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L;
    private static StreamExecutionEnvironmentFactory contextEnvironmentFactory;

    public StreamExecutionEnvironment() {
        this.timeCharacteristic = DEFAULT_TIME_CHARACTERISTIC;
        this.cacheFile = new ArrayList();
    }
}
