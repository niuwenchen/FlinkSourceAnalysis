package com.jackniu.flink.api.common.typeutils.base;

import com.jackniu.flink.api.common.typeutils.CompositeSerializerSnapshot;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import com.jackniu.flink.api.common.typeutils.TypeSerializerSnapshot;
import com.jackniu.flink.api.java.typeutils.runtime.DataInputViewStream;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.InstantiationUtil;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.jackniu.flink.util.Preconditions.checkState;

/**
 * Created by JackNiu on 2019/6/27.
 */
public final class GenericArraySerializerConfigSnapshot<C> implements TypeSerializerSnapshot<C[]> {
    private static final int CURRENT_VERSION = 2;

    /** The class of the components of the serializer's array type. */
    @Nullable
    private Class<C> componentClass;

    /** Snapshot handling for the component serializer snapshot. */
    @Nullable
    private CompositeSerializerSnapshot nestedSnapshot;

    /**
     * Constructor for read instantiation.
     */
    @SuppressWarnings("unused")
    public GenericArraySerializerConfigSnapshot() {}

    /**
     * Constructor to create the snapshot for writing.
     */
    public GenericArraySerializerConfigSnapshot(GenericArraySerializer<C> serializer) {
        this.componentClass = serializer.getComponentClass();
        this.nestedSnapshot = new CompositeSerializerSnapshot(serializer.getComponentSerializer());
    }

    // ------------------------------------------------------------------------

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        checkState(componentClass != null && nestedSnapshot != null);
        out.writeUTF(componentClass.getName());
        nestedSnapshot.writeCompositeSnapshot(out);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
        switch (readVersion) {
            case 1:
                readV1(in, classLoader);
                break;
            case 2:
                readV2(in, classLoader);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized version: " + readVersion);
        }
    }

    private void readV1(DataInputView in, ClassLoader classLoader) throws IOException {
        nestedSnapshot = CompositeSerializerSnapshot.legacyReadProductSnapshots(in, classLoader);

        try (DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
            componentClass = InstantiationUtil.deserializeObject(inViewWrapper, classLoader);
        }
        catch (ClassNotFoundException e) {
            throw new IOException("Could not find requested element class in classpath.", e);
        }
    }

    private void readV2(DataInputView in, ClassLoader classLoader) throws IOException {
        componentClass = InstantiationUtil.resolveClassByName(in, classLoader);
        nestedSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, classLoader);
    }

    @Override
    public TypeSerializer<C[]> restoreSerializer() {
        checkState(componentClass != null && nestedSnapshot != null);
        return new GenericArraySerializer<>(componentClass, nestedSnapshot.getRestoreSerializer(0));
    }

    @Override
    public TypeSerializerSchemaCompatibility<C[]> resolveSchemaCompatibility(TypeSerializer<C[]> newSerializer) {
        checkState(componentClass != null && nestedSnapshot != null);

        if (newSerializer instanceof GenericArraySerializer) {
            GenericArraySerializer<C> serializer = (GenericArraySerializer<C>) newSerializer;
            TypeSerializerSchemaCompatibility<C> compat = serializer.getComponentClass() == componentClass ?
                    TypeSerializerSchemaCompatibility.compatibleAsIs() :
                    TypeSerializerSchemaCompatibility.incompatible();

            return nestedSnapshot.resolveCompatibilityWithNested(
                    compat, serializer.getComponentSerializer());
        }
        else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
