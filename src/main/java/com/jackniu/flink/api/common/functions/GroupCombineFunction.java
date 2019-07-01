package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface GroupCombineFunction<IN, OUT> extends Function, Serializable {
    void combine(Iterable<IN> values, Collector<OUT> out) throws Exception;
}
