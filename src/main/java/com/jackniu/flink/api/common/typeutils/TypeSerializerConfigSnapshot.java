package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.io.VersionedIOReadableWritable;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.Preconditions;

import java.io.IOException;

import static com.jackniu.flink.util.Preconditions.checkState;

/**
 * Created by JackNiu on 2019/6/19.
 */
public abstract class TypeSerializerConfigSnapshot<T>  extends VersionedIOReadableWritable implements TypeSerializerSnapshot<T>{
    /** Version / Magic number for the format that bridges between the old and new interface. */
    private static final int ADAPTER_VERSION = 0x7a53c4f0;

    /** The user code class loader; only relevant if this configuration instance was deserialized from binary form. */
    private ClassLoader userCodeClassLoader;

    /** The originating serializer of this configuration snapshot. */
    private TypeSerializer<T> serializer;

    public final void setPriorSerializer(TypeSerializer<T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
    public final void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
    }

    public final ClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    @Override
    public final int getCurrentVersion() {
        return ADAPTER_VERSION;
    }

    @Override
    public final void writeSnapshot(DataOutputView out) throws IOException {
        checkState(serializer != null, "the prior serializer has not been set on this");
        // write the snapshot for a non-updated serializer.
        // this mimics the previous behavior where the TypeSerializer was
        // Java-serialized, for backwards compatibility
        TypeSerializerSerializationUtil.writeSerializer(out, serializer);

        // now delegate to the snapshots own writing code
        write(out);

    }

    @Override
    public final void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        if (readVersion != ADAPTER_VERSION) {
            throw new IOException("Wrong/unexpected version for the TypeSerializerConfigSnapshot: " + readVersion);
        }

        serializer = TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true);

        // now delegate to the snapshots own reading code
        setUserCodeClassLoader(userCodeClassLoader);
        read(in);
    }

    @Override
    public final TypeSerializer<T> restoreSerializer() {
        if (serializer == null) {
            throw new IllegalStateException(
                    "Trying to restore the prior serializer via TypeSerializerConfigSnapshot, " +
                            "but the prior serializer has not been set.");
        }
        else if (serializer instanceof UnloadableDummyTypeSerializer) {
            Throwable originalError = ((UnloadableDummyTypeSerializer<?>) serializer).getOriginalError();

            throw new IllegalStateException(
                    "Could not Java-deserialize TypeSerializer while restoring checkpoint metadata for serializer " +
                            "snapshot '" + getClass().getName() + "'. " +
                            "Please update to the TypeSerializerSnapshot interface that removes Java Serialization to avoid " +
                            "this problem in the future.", originalError);
        } else {
            return this.serializer;
        }
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {

        // in prior versions, the compatibility check was in the serializer itself, so we
        // delegate this call to the serializer.
        final CompatibilityResult<T> compatibility = newSerializer.ensureCompatibility(this);

        return compatibility.isRequiresMigration() ?
                TypeSerializerSchemaCompatibility.incompatible() :
                TypeSerializerSchemaCompatibility.compatibleAsIs();
    }




}
