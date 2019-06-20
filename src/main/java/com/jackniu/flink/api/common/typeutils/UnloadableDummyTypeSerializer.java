package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class UnloadableDummyTypeSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 2526330533671642711L;

    private final byte[] actualBytes;

    @Nullable
    private final Throwable originalError;

    public UnloadableDummyTypeSerializer(byte[] actualBytes) {
        this(actualBytes, null);
    }

    public UnloadableDummyTypeSerializer(byte[] actualBytes, @Nullable Throwable originalError) {
        this.actualBytes = Preconditions.checkNotNull(actualBytes);
        this.originalError = originalError;
    }

    public byte[] getActualBytes() {
        return actualBytes;
    }

    @Nullable
    public Throwable getOriginalError() {
        return originalError;
    }

    @Override
    public boolean isImmutableType() {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public TypeSerializer<T> duplicate() {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public T createInstance() {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public T copy(T from) {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public T copy(T from, T reuse) {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public int getLength() {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public TypeSerializerConfigSnapshot<T> snapshotConfiguration() {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
        throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnloadableDummyTypeSerializer<?> that = (UnloadableDummyTypeSerializer<?>) o;

        return Arrays.equals(getActualBytes(), that.getActualBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getActualBytes());
    }

}
