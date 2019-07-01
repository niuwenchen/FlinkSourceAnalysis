package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface CoGroupFunction<IN1, IN2, O> extends Function, Serializable {
    void coGroup(Iterable<IN1> first, Iterable<IN2> second, Collector<O> out) throws Exception;
}
