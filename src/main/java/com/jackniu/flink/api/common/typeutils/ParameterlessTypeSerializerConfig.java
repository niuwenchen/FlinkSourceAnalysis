package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.Preconditions;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/20.
 */
public final class ParameterlessTypeSerializerConfig<T> extends TypeSerializerConfigSnapshot<T> {
    private static final int VERSION = 1;

    /**
     * A string identifier that encodes the serialization format used by the serializer.
     *
     * TODO we might change this to a proper serialization format class in the future
     */
    private String serializationFormatIdentifier;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public ParameterlessTypeSerializerConfig() {}

    public ParameterlessTypeSerializerConfig(String serializationFormatIdentifier) {
        this.serializationFormatIdentifier = Preconditions.checkNotNull(serializationFormatIdentifier);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);
        out.writeUTF(serializationFormatIdentifier);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);
        serializationFormatIdentifier = in.readUTF();
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    public String getSerializationFormatIdentifier() {
        return serializationFormatIdentifier;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        return (other instanceof ParameterlessTypeSerializerConfig)
                && serializationFormatIdentifier.equals(((ParameterlessTypeSerializerConfig) other).getSerializationFormatIdentifier());
    }

    @Override
    public int hashCode() {
        return serializationFormatIdentifier.hashCode();
    }
}
