package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.aggregators.Aggregator;
import com.jackniu.flink.types.Value;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface IterationRuntimeContext extends RuntimeContext {

    /**
     * Gets the number of the current superstep. Superstep numbers start at <i>1</i>.
     *
     * @return The number of the current superstep.
     */
    int getSuperstepNumber();

    @PublicEvolving
    <T extends Aggregator<?>> T getIterationAggregator(String name);

    <T extends Value> T getPreviousIterationAggregate(String name);
}

