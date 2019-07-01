package com.jackniu.flink.api.common.functions;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface JoinFunction<IN1, IN2, OUT> extends Function, Serializable {

    OUT join(IN1 first, IN2 second) throws Exception;
}
