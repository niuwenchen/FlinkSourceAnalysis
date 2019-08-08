package com.jackniu.flink.api.common.state;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface FoldingState<T, ACC> extends AppendingState<T, ACC> {}
