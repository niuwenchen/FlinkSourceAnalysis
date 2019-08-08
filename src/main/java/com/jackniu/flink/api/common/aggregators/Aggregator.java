package com.jackniu.flink.api.common.aggregators;

import com.jackniu.flink.types.Value;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface Aggregator<T extends Value> extends Serializable {

    /**
     * Gets the aggregator's current aggregate.
     *
     * @return The aggregator's current aggregate.
     */
    T getAggregate();

    /**
     * Aggregates the given element. In the case of a <i>sum</i> aggregator, this method adds the given
     * value to the sum.
     *
     * @param element The element to aggregate.
     */
    void aggregate(T element);

    /**
     * Resets the internal state of the aggregator. This must bring the aggregator into the same
     * state as if it was newly initialized.
     */
    void reset();
}

