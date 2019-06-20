package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.Preconditions;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class TypeDeserializerAdapter<T> extends TypeSerializer<T> implements TypeDeserializer<T> {
    private static final long serialVersionUID = 1L;

    /** The actual wrapped deserializer or serializer instance */
    private final TypeDeserializer<T> deserializer;
    private final TypeSerializer<T> serializer;

    /**
     * Creates a {@link TypeDeserializerAdapter} that wraps a {@link TypeDeserializer}.
     *
     * @param deserializer the actual deserializer to wrap.
     */
    public TypeDeserializerAdapter(TypeDeserializer<T> deserializer) {
        this.deserializer = Preconditions.checkNotNull(deserializer);
        this.serializer = null;
    }
    public TypeDeserializerAdapter(TypeSerializer<T> serializer) {
        this.deserializer = null;
        this.serializer = Preconditions.checkNotNull(serializer);
    }

    public T deserialize(DataInputView source) throws IOException {
        return (deserializer != null) ? deserializer.deserialize(source) : serializer.deserialize(source);
    }

    public T deserialize(T reuse, DataInputView source) throws IOException {
        return (deserializer != null) ? deserializer.deserialize(reuse, source) : serializer.deserialize(reuse, source);
    }

    public TypeSerializer<T> duplicate() {
        return (deserializer != null) ? deserializer.duplicate() : serializer.duplicate();
    }

    public int getLength() {
        return (deserializer != null) ? deserializer.getLength() : serializer.getLength();
    }

    public boolean equals(Object obj) {
        return (deserializer != null) ? deserializer.equals(obj) : serializer.equals(obj);
    }

    public boolean canEqual(Object obj) {
        return (deserializer != null) ? deserializer.canEqual(obj) : serializer.canEqual(obj);
    }

    public int hashCode() {
        return (deserializer != null) ? deserializer.hashCode() : serializer.hashCode();
    }


    // --------------------------------------------------------------------------------------------
    // Irrelevant methods not intended for use
    // --------------------------------------------------------------------------------------------

    public boolean isImmutableType() {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }

    public T createInstance() {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }

    public T copy(T from) {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }

    public T copy(T from, T reuse) {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }

    public void serialize(T record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }

    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }

    public TypeSerializerConfigSnapshot<T> snapshotConfiguration() {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }

    public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
        throw new UnsupportedOperationException(
                "This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
    }



}
