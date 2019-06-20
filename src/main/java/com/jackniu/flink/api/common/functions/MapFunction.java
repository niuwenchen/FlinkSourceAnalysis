package com.jackniu.flink.api.common.functions;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/20.
 */
public interface MapFunction<T, O> extends Function, Serializable {
    O map(T value) throws Exception;
}
