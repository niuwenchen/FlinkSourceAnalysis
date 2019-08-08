package com.jackniu.flink.api.common.state;

import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.common.typeutils.base.ListSerializer;
import com.jackniu.flink.api.java.typeutils.ListTypeInfo;

import java.util.List;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class ListStateDescriptor<T> extends StateDescriptor<ListState<T>, List<T>> {
    private static final long serialVersionUID = 2L;

    /**
     * Creates a new {@code ListStateDescriptor} with the given name and list element type.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #ListStateDescriptor(String, TypeInformation)} constructor.
     *
     * @param name The (unique) name for the state.
     * @param elementTypeClass The type of the elements in the state.
     */
    public ListStateDescriptor(String name, Class<T> elementTypeClass) {
        super(name, new ListTypeInfo<>(elementTypeClass), null);
    }

    /**
     * Creates a new {@code ListStateDescriptor} with the given name and list element type.
     *
     * @param name The (unique) name for the state.
     * @param elementTypeInfo The type of the elements in the state.
     */
    public ListStateDescriptor(String name, TypeInformation<T> elementTypeInfo) {
        super(name, new ListTypeInfo<>(elementTypeInfo), null);
    }

    /**
     * Creates a new {@code ListStateDescriptor} with the given name and list element type.
     *
     * @param name The (unique) name for the state.
     * @param typeSerializer The type serializer for the list values.
     */
    public ListStateDescriptor(String name, TypeSerializer<T> typeSerializer) {
        super(name, new ListSerializer<>(typeSerializer), null);
    }

    /**
     * Gets the serializer for the elements contained in the list.
     *
     * @return The serializer for the elements in the list.
     */
    public TypeSerializer<T> getElementSerializer() {
        // call getSerializer() here to get the initialization check and proper error message
        final TypeSerializer<List<T>> rawSerializer = getSerializer();
        if (!(rawSerializer instanceof ListSerializer)) {
            throw new IllegalStateException();
        }

        return ((ListSerializer<T>) rawSerializer).getElementSerializer();
    }

    @Override
    public Type getType() {
        return Type.LIST;
    }
}
