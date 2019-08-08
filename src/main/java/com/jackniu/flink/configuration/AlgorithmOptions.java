package com.jackniu.flink.configuration;

import static com.jackniu.flink.configuration.ConfigOptions.key;

/**
 * Created by JackNiu on 2019/7/8.
 */
public class AlgorithmOptions {
    public static final ConfigOption<Boolean> HASH_JOIN_BLOOM_FILTERS =
            key("taskmanager.runtime.hashjoin-bloom-filters")
                    .defaultValue(false)
                    .withDescription("Flag to activate/deactivate bloom filters in the hybrid hash join implementation." +
                            " In cases where the hash join needs to spill to disk (datasets larger than the reserved fraction of" +
                            " memory), these bloom filters can greatly reduce the number of spilled records, at the cost some" +
                            " CPU cycles.");

    public static final ConfigOption<Integer> SPILLING_MAX_FAN =
            key("taskmanager.runtime.max-fan")
                    .defaultValue(128)
                    .withDescription("The maximal fan-in for external merge joins and fan-out for spilling hash tables. Limits" +
                            " the number of file handles per operator, but may cause intermediate merging/partitioning, if set too" +
                            " small.");

    public static final ConfigOption<Float> SORT_SPILLING_THRESHOLD =
            key("taskmanager.runtime.sort-spilling-threshold")
                    .defaultValue(0.8f)
                    .withDescription("A sort operation starts spilling when this fraction of its memory budget is full.");
}

