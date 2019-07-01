package com.jackniu.flink.api.common.functions;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {
    ACC createAccumulator();
    ACC add(IN value, ACC accumulator);
    OUT getResult(ACC accumulator);
    ACC merge(ACC a, ACC b);
}
