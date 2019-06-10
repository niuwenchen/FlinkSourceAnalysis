package com.jackniu.flink.streaming.api.environment;

/**
 * Created by JackNiu on 2019/6/6.
 */
public interface StreamExecutionEnvironmentFactory {
    StreamExecutionEnvironment createExecutionEnvironment();
}
