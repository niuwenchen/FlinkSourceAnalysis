package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface GroupReduceFunction<T, O> extends Function, Serializable {
    void reduce(Iterable<T> values, Collector<O> out) throws Exception;
}
