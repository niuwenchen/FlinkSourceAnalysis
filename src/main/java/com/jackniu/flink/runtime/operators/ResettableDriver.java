package com.jackniu.flink.runtime.operators;

import com.jackniu.flink.api.common.functions.Function;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ResettableDriver<S extends Function, OT> extends Driver<S, OT> {

    boolean isInputResettable(int inputNum);

    void initialize() throws Exception;

    void reset() throws Exception;

    void teardown() throws Exception;
}