package com.jackniu.flink.api.common.functions;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface CrossFunction<IN1, IN2, OUT> extends Function, Serializable {

    /**
     * Cross UDF method. Called once per pair of elements in the Cartesian product of the inputs.
     *
     * @param val1 Element from first input.
     * @param val2 Element from the second input.
     * @return The result element.
     *
     * @throws Exception The function may throw Exceptions, which will cause the program to cancel,
     *                   and may trigger the recovery logic.
     */
    OUT cross(IN1 val1, IN2 val2) throws Exception;

}
