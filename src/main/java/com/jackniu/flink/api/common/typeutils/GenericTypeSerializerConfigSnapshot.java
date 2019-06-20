package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.Preconditions;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class GenericTypeSerializerConfigSnapshot<T> extends TypeSerializerConfigSnapshot {
    private Class<T> typeClass;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public GenericTypeSerializerConfigSnapshot() {}

    public GenericTypeSerializerConfigSnapshot(Class<T> typeClass) {
        this.typeClass = Preconditions.checkNotNull(typeClass);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);

        // write only the classname to avoid Java serialization
        out.writeUTF(typeClass.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);

        String genericTypeClassname = in.readUTF();
        try {
            typeClass = (Class<T>) Class.forName(genericTypeClassname, true, getUserCodeClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not find the requested class " + genericTypeClassname + " in classpath.", e);
        }
    }

    public Class<T> getTypeClass() {
        return typeClass;
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
                && typeClass.equals(((GenericTypeSerializerConfigSnapshot) obj).getTypeClass());
    }

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }
}
