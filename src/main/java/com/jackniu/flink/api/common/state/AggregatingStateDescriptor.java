package com.jackniu.flink.api.common.state;

import com.jackniu.flink.api.common.functions.AggregateFunction;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class AggregatingStateDescriptor<IN, ACC, OUT> extends StateDescriptor<AggregatingState<IN, OUT>, ACC> {
    private static final long serialVersionUID = 1L;

    /** The aggregation function for the state. */
    private final AggregateFunction<IN, ACC, OUT> aggFunction;

    /**
     * Creates a new state descriptor with the given name, function, and type.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #AggregatingStateDescriptor(String, AggregateFunction, TypeInformation)} constructor.
     *
     * @param name The (unique) name for the state.
     * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
     * @param stateType The type of the accumulator. The accumulator is stored in the state.
     */
    public AggregatingStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            Class<ACC> stateType) {

        super(name, stateType, null);
        this.aggFunction = checkNotNull(aggFunction);
    }

    /**
     * Creates a new {@code ReducingStateDescriptor} with the given name and default value.
     *
     * @param name The (unique) name for the state.
     * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
     * @param stateType The type of the accumulator. The accumulator is stored in the state.
     */
    public AggregatingStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeInformation<ACC> stateType) {

        super(name, stateType, null);
        this.aggFunction = checkNotNull(aggFunction);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and default value.
     *
     * @param name The (unique) name for the state.
     * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
     * @param typeSerializer The serializer for the accumulator. The accumulator is stored in the state.
     */
    public AggregatingStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<ACC> typeSerializer) {

        super(name, typeSerializer, null);
        this.aggFunction = checkNotNull(aggFunction);
    }

    /**
     * Returns the aggregate function to be used for the state.
     */
    public AggregateFunction<IN, ACC, OUT> getAggregateFunction() {
        return aggFunction;
    }

    @Override
    public Type getType() {
        return Type.AGGREGATING;
    }
}

