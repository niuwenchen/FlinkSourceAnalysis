package com.jackniu.flink.api.common.accumulators;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface SimpleAccumulator<T extends Serializable> extends Accumulator<T,T> {}
