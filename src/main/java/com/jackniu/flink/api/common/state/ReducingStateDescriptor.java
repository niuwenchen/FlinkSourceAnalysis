package com.jackniu.flink.api.common.state;

import com.jackniu.flink.api.common.functions.ReduceFunction;
import com.jackniu.flink.api.common.functions.RichFunction;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class ReducingStateDescriptor<T> extends StateDescriptor<ReducingState<T>, T> {

    private static final long serialVersionUID = 1L;

    private final ReduceFunction<T> reduceFunction;

    /**
     * Creates a new {@code ReducingStateDescriptor} with the given name, type, and default value.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #ReducingStateDescriptor(String, ReduceFunction, TypeInformation)} constructor.
     *
     * @param name The (unique) name for the state.
     * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
     * @param typeClass The type of the values in the state.
     */
    public ReducingStateDescriptor(String name, ReduceFunction<T> reduceFunction, Class<T> typeClass) {
        super(name, typeClass, null);
        this.reduceFunction = checkNotNull(reduceFunction);

        if (reduceFunction instanceof RichFunction) {
            throw new UnsupportedOperationException("ReduceFunction of ReducingState can not be a RichFunction.");
        }
    }



    /**
     * Creates a new {@code ReducingStateDescriptor} with the given name and default value.
     *
     * @param name The (unique) name for the state.
     * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
     * @param typeInfo The type of the values in the state.
     */
    public ReducingStateDescriptor(String name, ReduceFunction<T> reduceFunction, TypeInformation<T> typeInfo) {
        super(name, typeInfo, null);
        this.reduceFunction = checkNotNull(reduceFunction);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and default value.
     *
     * @param name The (unique) name for the state.
     * @param reduceFunction The {@code ReduceFunction} used to aggregate the state.
     * @param typeSerializer The type serializer of the values in the state.
     */
    public ReducingStateDescriptor(String name, ReduceFunction<T> reduceFunction, TypeSerializer<T> typeSerializer) {
        super(name, typeSerializer, null);
        this.reduceFunction = checkNotNull(reduceFunction);
    }

    /**
     * Returns the reduce function to be used for the reducing state.
     */
    public ReduceFunction<T> getReduceFunction() {
        return reduceFunction;
    }

    @Override
    public Type getType() {
        return Type.REDUCING;
    }
}
