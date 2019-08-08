package com.jackniu.flink.runtime.plugable;

import com.jackniu.flink.core.io.IOReadableWritable;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface DeserializationDelegate<T> extends IOReadableWritable {

    void setInstance(T instance);

    T getInstance();
}