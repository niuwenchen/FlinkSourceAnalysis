package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface FlatJoinFunction<IN1, IN2, OUT> extends Function, Serializable {
    void join (IN1 first, IN2 second, Collector<OUT> out) throws Exception;
}
