package com.jackniu.flink.types;

/**
 * Created by JackNiu on 2019/6/19.
 */
public interface ResettableValue<T extends Value> extends Value {
    void setValue(T value);
}
