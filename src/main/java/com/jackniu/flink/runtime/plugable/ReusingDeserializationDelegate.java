package com.jackniu.flink.runtime.plugable;

import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class ReusingDeserializationDelegate<T> implements DeserializationDelegate<T> {

    private T instance;

    private final TypeSerializer<T> serializer;

    public ReusingDeserializationDelegate(TypeSerializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void setInstance(T instance) {
        this.instance = instance;
    }

    @Override
    public T getInstance() {
        return instance;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new IllegalStateException("Serialization method called on DeserializationDelegate.");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.instance = this.serializer.deserialize(this.instance, in);
    }
}
