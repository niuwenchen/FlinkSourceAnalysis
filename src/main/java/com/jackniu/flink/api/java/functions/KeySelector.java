package com.jackniu.flink.api.java.functions;

import com.jackniu.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface KeySelector<IN,KEY> extends Function,Serializable {
    KEY getKey(IN value) throws Exception;
}
