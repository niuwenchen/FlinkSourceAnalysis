package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/28.
 */
public class BackwardsCompatibleSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {
    @Nonnull
    private TypeSerializer<T> serializerInstance;

    public BackwardsCompatibleSerializerSnapshot(TypeSerializer<T> serializerInstance) {
        this.serializerInstance = Preconditions.checkNotNull(serializerInstance);
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException(
                "This is a dummy config snapshot used only for backwards compatibility.");
    }

    @Override
    public void readSnapshot(int version, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        throw new UnsupportedOperationException(
                "This is a dummy config snapshot used only for backwards compatibility.");
    }

    @Override
    public int getCurrentVersion() {
        throw new UnsupportedOperationException(
                "This is a dummy config snapshot used only for backwards compatibility.");
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return serializerInstance;
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
        // if there is no configuration snapshot to check against,
        // then we can only assume that the new serializer is compatible as is
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    @Override
    public int hashCode() {
        return serializerInstance.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BackwardsCompatibleSerializerSnapshot<?> that = (BackwardsCompatibleSerializerSnapshot<?>) o;

        return that.serializerInstance.equals(serializerInstance);
    }
}
