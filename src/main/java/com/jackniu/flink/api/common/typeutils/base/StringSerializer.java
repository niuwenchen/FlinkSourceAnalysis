package com.jackniu.flink.api.common.typeutils.base;

import com.jackniu.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import com.jackniu.flink.api.common.typeutils.TypeSerializerSnapshot;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.types.StringValue;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/18.
 */
public final class StringSerializer extends TypeSerializerSingleton<String>  {
    /**
     * EMPTY
     * isImmutableType
     * createInstance
     * copy getLength
     * serialize
     *
     * */
    private static final long serialVersionUID = 1L;

    /** Sharable instance of the StringSerializer. */
    public static final StringSerializer INSTANCE = new StringSerializer();

    private static final String EMPTY = "";

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public String createInstance() {
        return EMPTY;
    }

    @Override
    public String copy(String from) {
        return from;
    }

    @Override
    public String copy(String from, String reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(String record, DataOutputView target) throws IOException {
        StringValue.writeString(record, target);
    }

    @Override
    public String deserialize(DataInputView source) throws IOException {
        return StringValue.readString(source);
    }

    @Override
    public String deserialize(String record, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        StringValue.copyString(source, target);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof StringSerializer;
    }

    @Override
    public TypeSerializerSnapshot<String> snapshotConfiguration() {
        return new StringSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /**
     * Serializer configuration snapshot for compatibility and format evolution.
     */
    public static final class StringSerializerSnapshot extends SimpleTypeSerializerSnapshot<String> {

        public StringSerializerSnapshot() {
            super(StringSerializer.class);
        }
    }
}
