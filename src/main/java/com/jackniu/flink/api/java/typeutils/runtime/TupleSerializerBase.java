package com.jackniu.flink.api.java.typeutils.runtime;

import com.jackniu.flink.annotations.VisibleForTesting;
import com.jackniu.flink.api.common.typeutils.*;
import com.jackniu.flink.api.java.tuple.Tuple2;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/6/28.
 */
public  abstract class TupleSerializerBase<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 1L;

    protected final Class<T> tupleClass;

    protected TypeSerializer<Object>[] fieldSerializers;

    protected final int arity;

    private int length = -2;

    @SuppressWarnings("unchecked")
    public TupleSerializerBase(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
        this.tupleClass = checkNotNull(tupleClass);
        this.fieldSerializers = (TypeSerializer<Object>[]) checkNotNull(fieldSerializers);
        this.arity = fieldSerializers.length;
    }
    public Class<T> getTupleClass() {
        return this.tupleClass;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        if (length == -2) {
            int sum = 0;
            for (TypeSerializer<Object> serializer : fieldSerializers) {
                if (serializer.getLength() > 0) {
                    sum += serializer.getLength();
                } else {
                    length = -1;
                    return length;
                }
            }
            length = sum;
        }
        return length;
    }

    public int getArity() {
        return arity;
    }

    // We use this in the Aggregate and Distinct Operators to create instances
    // of immutable Tuples (i.e. Scala Tuples)
    public abstract T createInstance(Object[] fields);

    public abstract T createOrReuseInstance(Object[] fields, T reuse);

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        for (int i = 0; i < arity; i++) {
            fieldSerializers[i].copy(source, target);
        }
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(fieldSerializers) + Objects.hash(tupleClass, arity);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TupleSerializerBase) {
            TupleSerializerBase<?> other = (TupleSerializerBase<?>) obj;

            return other.canEqual(this) &&
                    tupleClass == other.tupleClass &&
                    Arrays.equals(fieldSerializers, other.fieldSerializers) &&
                    arity == other.arity;
        } else {
            return false;
        }
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof TupleSerializerBase;
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TupleSerializerConfigSnapshot<T> snapshotConfiguration() {
        return new TupleSerializerConfigSnapshot<>(tupleClass, fieldSerializers);
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
        if (configSnapshot instanceof TupleSerializerConfigSnapshot) {
            final TupleSerializerConfigSnapshot<T> config = (TupleSerializerConfigSnapshot<T>) configSnapshot;

            if (tupleClass.equals(config.getTupleClass())) {
                List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousFieldSerializersAndConfigs =
                        ((TupleSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

                if (previousFieldSerializersAndConfigs.size() == fieldSerializers.length) {

                    TypeSerializer<Object>[] convertFieldSerializers = new TypeSerializer[fieldSerializers.length];
                    boolean requiresMigration = false;
                    CompatibilityResult<Object> compatResult;
                    int i = 0;
                    for (Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> f : previousFieldSerializersAndConfigs) {
                        compatResult = CompatibilityUtil.resolveCompatibilityResult(
                                f.f0,
                                UnloadableDummyTypeSerializer.class,
                                f.f1,
                                fieldSerializers[i]);

                        if (compatResult.isRequiresMigration()) {
                            requiresMigration = true;

                            if (compatResult.getConvertDeserializer() != null) {
                                convertFieldSerializers[i] =
                                        new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer());
                            } else {
                                return CompatibilityResult.requiresMigration();
                            }
                        }

                        i++;
                    }

                    if (!requiresMigration) {
                        return CompatibilityResult.compatible();
                    } else {
                        return CompatibilityResult.requiresMigration(
                                createSerializerInstance(tupleClass, convertFieldSerializers));
                    }
                }
            }
        }

        return CompatibilityResult.requiresMigration();
    }

    protected abstract TupleSerializerBase<T> createSerializerInstance(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers);

    @VisibleForTesting
    public TypeSerializer<Object>[] getFieldSerializers() {
        return fieldSerializers;
    }

}
