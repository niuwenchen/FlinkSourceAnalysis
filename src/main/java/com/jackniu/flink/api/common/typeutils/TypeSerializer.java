package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/18.
 */
public abstract class TypeSerializer<T> implements Serializable{

    private static final long serialVersionUID = 1L;

    public abstract boolean isImmutableType();


    /**
     * Creates a deep copy of this serializer if it is necessary, i.e. if it is stateful. This
     * can return itself if the serializer is not stateful.
     *
     * We need this because Serializers might be used in several threads. Stateless serializers
     * are inherently thread-safe while stateful serializers might not be thread-safe.
     */
    public abstract TypeSerializer<T> duplicate();

    public abstract T createInstance();

    /**
     * Creates a deep copy of the given element in a new element.
     *
     * @param from The element reuse be copied.
     * @return A deep copy of the element.
     */
    public abstract T copy(T from);

    /**
     * Creates a copy from the given element.
     * The method makes an attempt to store the copy in the given reuse element, if the type is mutable.
     * This is, however, not guaranteed.
     *
     * @param from The element to be copied.
     * @param reuse The element to be reused. May or may not be used.
     * @return A deep copy of the element.
     */
    public abstract T copy(T from, T reuse);

    /**
     * Gets the length of the data type, if it is a fix length data type.
     *
     * @return The length of the data type, or <code>-1</code> for variable length data types.
     */
    public abstract int getLength();

    /**
     * Serializes the given record to the given target output view.
     *
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     *
     * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically raised by the
     *                     output view, which may have an underlying I/O channel to which it delegates.
     */
    public abstract void serialize(T record, DataOutputView target) throws IOException;

    /**
     * De-serializes a record from the given source input view.
     *
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     *
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
     *                     input view, which may have an underlying I/O channel from which it reads.
     */
    public abstract T deserialize(DataInputView source) throws IOException;

    /**
     * De-serializes a record from the given source input view into the given reuse record instance if mutable.
     *
     * @param reuse The record instance into which to de-serialize the data.
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     *
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
     *                     input view, which may have an underlying I/O channel from which it reads.
     */
    public abstract T deserialize(T reuse, DataInputView source) throws IOException;


    public abstract void copy(DataInputView source, DataOutputView target) throws IOException;

    public abstract boolean equals(Object obj);

    public abstract boolean canEqual(Object obj);

    public abstract int hashCode();

/**
 * Snapshots the configuration of this TypeSerializer. This method is only relevant if the serializer is
 * used to state stored in checkpoints/savepoints.
 *
 * <p>The snapshot of the TypeSerializer is supposed to contain all information that affects the serialization
 * format of the serializer. The snapshot serves two purposes: First, to reproduce the serializer when the
 * checkpoint/savepoint is restored, and second, to check whether the serialization format is compatible
 * with the serializer used in the restored program.
 *
 * <p><b>IMPORTANT:</b> TypeSerializerSnapshots changed after Flink 1.6. Serializers implemented against
 * Flink versions up to 1.6 should still work, but adjust to new model to enable state evolution and be
 * future-proof.
 * See the class-level comments, section "Upgrading TypeSerializers to the new TypeSerializerSnapshot model"
 * for details.
 *
 */
    public abstract TypeSerializerSnapshot<T> snapshotConfiguration();

    @Deprecated
    public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
        throw new UnsupportedOperationException(
                "This method is not supported any more - please evolve your TypeSerializer the following way:\n\n" +
                        "  - If you have a serializer whose 'ensureCompatibility()' method delegates to another\n" +
                        "    serializer's 'ensureCompatibility()', please use" +
                        "'CompatibilityUtil.resolveCompatibilityResult(snapshot, this)' instead.\n\n" +
                        "  - If you updated your serializer (removed overriding the 'ensureCompatibility()' method),\n" +
                        "    please also update the corresponding config snapshot to not extend 'TypeSerializerConfigSnapshot'" +
                        "any more.\n\n");
    }


}
