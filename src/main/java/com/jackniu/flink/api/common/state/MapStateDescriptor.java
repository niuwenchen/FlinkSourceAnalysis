package com.jackniu.flink.api.common.state;

import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.common.typeutils.base.MapSerializer;
import com.jackniu.flink.api.java.typeutils.MapTypeInfo;

import java.util.Map;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class MapStateDescriptor<UK, UV> extends StateDescriptor<MapState<UK, UV>, Map<UK, UV>> {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new {@code MapStateDescriptor} with the given name and the given type serializers.
     *
     * @param name The name of the {@code MapStateDescriptor}.
     * @param keySerializer The type serializer for the keys in the state.
     * @param valueSerializer The type serializer for the values in the state.
     */
    public MapStateDescriptor(String name, TypeSerializer<UK> keySerializer, TypeSerializer<UV> valueSerializer) {
        super(name, new MapSerializer<>(keySerializer, valueSerializer), null);
    }

    /**
     * Create a new {@code MapStateDescriptor} with the given name and the given type information.
     *
     * @param name The name of the {@code MapStateDescriptor}.
     * @param keyTypeInfo The type information for the keys in the state.
     * @param valueTypeInfo The type information for the values in the state.
     */
    public MapStateDescriptor(String name, TypeInformation<UK> keyTypeInfo, TypeInformation<UV> valueTypeInfo) {
        super(name, new MapTypeInfo<>(keyTypeInfo, valueTypeInfo), null);
    }

    /**
     * Create a new {@code MapStateDescriptor} with the given name and the given type information.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #MapStateDescriptor(String, TypeInformation, TypeInformation)} constructor.
     *
     * @param name The name of the {@code MapStateDescriptor}.
     * @param keyClass The class of the type of keys in the state.
     * @param valueClass The class of the type of values in the state.
     */
    public MapStateDescriptor(String name, Class<UK> keyClass, Class<UV> valueClass) {
        super(name, new MapTypeInfo<>(keyClass, valueClass), null);
    }

    @Override
    public Type getType() {
        return Type.MAP;
    }

    /**
     * Gets the serializer for the keys in the state.
     *
     * @return The serializer for the keys in the state.
     */
    public TypeSerializer<UK> getKeySerializer() {
        final TypeSerializer<Map<UK, UV>> rawSerializer = getSerializer();
        if (!(rawSerializer instanceof MapSerializer)) {
            throw new IllegalStateException("Unexpected serializer type.");
        }

        return ((MapSerializer<UK, UV>) rawSerializer).getKeySerializer();
    }

    /**
     * Gets the serializer for the values in the state.
     *
     * @return The serializer for the values in the state.
     */
    public TypeSerializer<UV> getValueSerializer() {
        final TypeSerializer<Map<UK, UV>> rawSerializer = getSerializer();
        if (!(rawSerializer instanceof MapSerializer)) {
            throw new IllegalStateException("Unexpected serializer type.");
        }

        return ((MapSerializer<UK, UV>) rawSerializer).getValueSerializer();
    }
}

