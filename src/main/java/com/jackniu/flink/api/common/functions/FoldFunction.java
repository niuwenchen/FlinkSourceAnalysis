package com.jackniu.flink.api.common.functions;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface FoldFunction<O,T> extends Function,Serializable {
    T fold(T accumulator, O value) throws Exception;
}
