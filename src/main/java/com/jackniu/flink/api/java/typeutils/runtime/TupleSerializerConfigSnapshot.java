package com.jackniu.flink.api.java.typeutils.runtime;

import com.jackniu.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.InstantiationUtil;
import com.jackniu.flink.util.Preconditions;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/28.
 */
public final  class TupleSerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot<T> {
    private static final int VERSION = 1;

    private Class<T> tupleClass;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public TupleSerializerConfigSnapshot() {}

    public TupleSerializerConfigSnapshot(Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
        super(fieldSerializers);

        this.tupleClass = Preconditions.checkNotNull(tupleClass);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);

        try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out)) {
            InstantiationUtil.serializeObject(outViewWrapper, tupleClass);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);

        try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
            tupleClass = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader(), true);
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not find requested tuple class in classpath.", e);
        }
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    public Class<T> getTupleClass() {
        return tupleClass;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
                && (obj instanceof TupleSerializerConfigSnapshot)
                && (tupleClass.equals(((TupleSerializerConfigSnapshot) obj).getTupleClass()));
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + tupleClass.hashCode();
    }

}
