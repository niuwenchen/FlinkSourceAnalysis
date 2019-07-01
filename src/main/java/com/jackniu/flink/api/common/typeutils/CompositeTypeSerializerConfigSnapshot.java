package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.api.java.tuple.Tuple2;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JackNiu on 2019/6/28.
 */
public abstract class CompositeTypeSerializerConfigSnapshot<T> extends TypeSerializerConfigSnapshot<T> {
    private List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> nestedSerializersAndConfigs;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public CompositeTypeSerializerConfigSnapshot() {}

    public CompositeTypeSerializerConfigSnapshot(TypeSerializer<?>... nestedSerializers) {
        Preconditions.checkNotNull(nestedSerializers);

        this.nestedSerializersAndConfigs = new ArrayList<>(nestedSerializers.length);
        for (TypeSerializer<?> nestedSerializer : nestedSerializers) {
            TypeSerializerSnapshot<?> configSnapshot = nestedSerializer.snapshotConfiguration();
            this.nestedSerializersAndConfigs.add(
                    new Tuple2<>(nestedSerializer.duplicate(), Preconditions.checkNotNull(configSnapshot)));
        }
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);
        TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(out, nestedSerializersAndConfigs);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);
        this.nestedSerializersAndConfigs =
                TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, getUserCodeClassLoader());
    }

    public List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> getNestedSerializersAndConfigs() {
        return nestedSerializersAndConfigs;
    }

    public Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>> getSingleNestedSerializerAndConfig() {
        return nestedSerializersAndConfigs.get(0);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        return (obj.getClass().equals(getClass()))
                && nestedSerializersAndConfigs.equals(((CompositeTypeSerializerConfigSnapshot) obj).getNestedSerializersAndConfigs());
    }

    @Override
    public int hashCode() {
        return nestedSerializersAndConfigs.hashCode();
    }


}
