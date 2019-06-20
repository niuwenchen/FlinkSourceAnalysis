package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.annotations.Public;
import com.jackniu.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/20.
 */
@Public
public interface FlatMapFunction<T, O> extends Function, Serializable {
    void flatMap(T value, Collector<O> out) throws Exception;
}
