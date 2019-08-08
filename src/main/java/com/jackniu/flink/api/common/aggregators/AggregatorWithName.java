package com.jackniu.flink.api.common.aggregators;

import com.jackniu.flink.types.Value;

/**
 * Created by JackNiu on 2019/7/8.
 */
public class AggregatorWithName<T extends Value> {

    private final String name;

    private final Aggregator<T> aggregator;

    /**
     * Creates a new instance for the given aggregator and name.
     *
     * @param name The name that the aggregator is registered under.
     * @param aggregator The aggregator.
     */
    public AggregatorWithName(String name, Aggregator<T> aggregator) {
        this.name = name;
        this.aggregator = aggregator;
    }

    /**
     * Gets the name that the aggregator is registered under.
     *
     * @return The name that the aggregator is registered under.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the aggregator.
     *
     * @return The aggregator.
     */
    public Aggregator<T> getAggregator() {
        return aggregator;
    }
}
