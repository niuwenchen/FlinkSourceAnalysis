package com.jackniu.flink.api.java.typeutils.runtime;

import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.java.tuple.Tuple0;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/28.
 */
public class Tuple0Serializer extends  TupleSerializer<Tuple0> {
    private static final long serialVersionUID = 1278813169022975971L;

    public static final Tuple0Serializer INSTANCE = new Tuple0Serializer();

    // ------------------------------------------------------------------------

    private Tuple0Serializer() {
        super(Tuple0.class, new TypeSerializer<?>[0]);
    }

    // ------------------------------------------------------------------------

    @Override
    public Tuple0Serializer duplicate() {
        return this;
    }

    @Override
    public Tuple0 createInstance() {
        return Tuple0.INSTANCE;
    }

    @Override
    public Tuple0 createInstance(Object[] fields) {
        if (fields == null || fields.length == 0) {
            return Tuple0.INSTANCE;
        }

        throw new UnsupportedOperationException(
                "Tuple0 cannot take any data, as it has zero fields.");
    }

    @Override
    public Tuple0 copy(Tuple0 from) {
        return from;
    }

    @Override
    public Tuple0 copy(Tuple0 from, Tuple0 reuse) {
        return reuse;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public void serialize(Tuple0 record, DataOutputView target) throws IOException {
        target.writeByte(42);
    }

    @Override
    public Tuple0 deserialize(DataInputView source) throws IOException {
        source.readByte();
        return Tuple0.INSTANCE;
    }

    @Override
    public Tuple0 deserialize(Tuple0 reuse, DataInputView source) throws IOException {
        source.readByte();
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte());
    }
    @Override
    public int hashCode() {
        return Tuple0Serializer.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Tuple0Serializer) {
            Tuple0Serializer other = (Tuple0Serializer) obj;

            return other.canEqual(this);
        } else {
            return false;
        }
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof Tuple0Serializer;
    }

    @Override
    public String toString() {
        return "Tuple0Serializer";
    }

}
