package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.InstantiationUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

import static com.jackniu.flink.util.Preconditions.checkNotNull;
import static com.jackniu.flink.util.Preconditions.checkState;

/**
 * Created by JackNiu on 2019/6/20.
 */
public abstract class SimpleTypeSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {
    private static final int CURRENT_VERSION = 2;

    @Nullable
    private Class<? extends TypeSerializer<T>> serializerClass;
    @SuppressWarnings("unused")
    public SimpleTypeSerializerSnapshot() {}

    /**
     * Constructor to create snapshot from serializer (writing the snapshot).
     */
    public SimpleTypeSerializerSnapshot(@Nonnull Class<? extends TypeSerializer<T>> serializerClass) {
        this.serializerClass = checkNotNull(serializerClass);
    }
// ------------------------------------------------------------------------
    //  Serializer Snapshot Methods
    // ------------------------------------------------------------------------

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        checkState(serializerClass != null);
        return InstantiationUtil.instantiate(serializerClass);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {

        checkState(serializerClass != null);
        return newSerializer.getClass() == serializerClass ?
                TypeSerializerSchemaCompatibility.compatibleAsIs() :
                TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        checkState(serializerClass != null);
        out.writeUTF(serializerClass.getName());
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
        switch (readVersion) {
            case 2:
                read(in, classLoader);
                break;
            default:
                throw new IOException("Unrecognized version: " + readVersion);
        }
    }

    private void read(DataInputView in, ClassLoader classLoader) throws IOException {
        final String className = in.readUTF();
        final Class<?> clazz = resolveClassName(className, classLoader, false);
        this.serializerClass = cast(clazz);
    }

    // ------------------------------------------------------------------------
    //  standard utilities
    // ------------------------------------------------------------------------

    @Override
    public final boolean equals(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public final int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return getClass().getName();
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static Class<?> resolveClassName(String className, ClassLoader cl, boolean allowCanonicalName) throws IOException {
        try {
            return Class.forName(className, false, cl);
        }
        catch (ClassNotFoundException e) {
            if (allowCanonicalName) {
                try {
                    return Class.forName(guessClassNameFromCanonical(className), false, cl);
                }
                catch (ClassNotFoundException ignored) {}
            }

            // throw with original ClassNotFoundException
            throw new IOException(
                    "Failed to read SimpleTypeSerializerSnapshot: Serializer class not found: " + className, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<? extends TypeSerializer<T>> cast(Class<?> clazz) throws IOException {
        if (!TypeSerializer.class.isAssignableFrom(clazz)) {
            throw new IOException("Failed to read SimpleTypeSerializerSnapshot. " +
                    "Serializer class name leads to a class that is not a TypeSerializer: " + clazz.getName());
        }

        return (Class<? extends TypeSerializer<T>>) clazz;
    }

    static String guessClassNameFromCanonical(String className) {
        int lastDot = className.lastIndexOf('.');
        if (lastDot > 0 && lastDot < className.length() - 1) {
            return className.substring(0, lastDot) + '$' + className.substring(lastDot + 1);
        } else {
            return className;
        }
    }


}
