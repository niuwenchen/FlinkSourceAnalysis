package com.jackniu.flink.api.common.aggregators;

import com.jackniu.flink.types.Value;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/8.
 */
public interface ConvergenceCriterion<T extends Value> extends Serializable {

    /**
     * Decide whether the iterative algorithm has converged
     */
    boolean isConverged(int iteration, T value);
}