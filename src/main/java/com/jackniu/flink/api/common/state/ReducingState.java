package com.jackniu.flink.api.common.state;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ReducingState<T> extends MergingState<T, T> {}
