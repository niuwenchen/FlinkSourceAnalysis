package com.jackniu.flink.api.common.state;

import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class ValueStateDescriptor<T> extends StateDescriptor<ValueState<T>, T> {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name, type, and default value.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #ValueStateDescriptor(String, TypeInformation, Object)} constructor.
     *
     * @deprecated Use {@link #ValueStateDescriptor(String, Class)} instead and manually manage
     * the default value by checking whether the contents of the state is {@code null}.
     *
     * @param name The (unique) name for the state.
     * @param typeClass The type of the values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *                     a value before.
     */
    @Deprecated
    public ValueStateDescriptor(String name, Class<T> typeClass, T defaultValue) {
        super(name, typeClass, defaultValue);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and default value.
     *
     * @deprecated Use {@link #ValueStateDescriptor(String, TypeInformation)} instead and manually
     * manage the default value by checking whether the contents of the state is {@code null}.
     *
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *                     a value before.
     */
    @Deprecated
    public ValueStateDescriptor(String name, TypeInformation<T> typeInfo, T defaultValue) {
        super(name, typeInfo, defaultValue);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name, default value, and the specific
     * serializer.
     *
     * @deprecated Use {@link #ValueStateDescriptor(String, TypeSerializer)} instead and manually
     * manage the default value by checking whether the contents of the state is {@code null}.
     *
     * @param name The (unique) name for the state.
     * @param typeSerializer The type serializer of the values in the state.
     * @param defaultValue The default value that will be set when requesting state without setting
     *                     a value before.
     */
    @Deprecated
    public ValueStateDescriptor(String name, TypeSerializer<T> typeSerializer, T defaultValue) {
        super(name, typeSerializer, defaultValue);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and type
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #ValueStateDescriptor(String, TypeInformation)} constructor.
     *
     * @param name The (unique) name for the state.
     * @param typeClass The type of the values in the state.
     */
    public ValueStateDescriptor(String name, Class<T> typeClass) {
        super(name, typeClass, null);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and type.
     *
     * @param name The (unique) name for the state.
     * @param typeInfo The type of the values in the state.
     */
    public ValueStateDescriptor(String name, TypeInformation<T> typeInfo) {
        super(name, typeInfo, null);
    }

    /**
     * Creates a new {@code ValueStateDescriptor} with the given name and the specific serializer.
     *
     * @param name The (unique) name for the state.
     * @param typeSerializer The type serializer of the values in the state.
     */
    public ValueStateDescriptor(String name, TypeSerializer<T> typeSerializer) {
        super(name, typeSerializer, null);
    }

    @Override
    public Type getType() {
        return Type.VALUE;
    }
}
