package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface MapPartitionFunction<T, O> extends Function, Serializable {
    void mapPartition(Iterable<T> values, Collector<O> out) throws Exception;
}
