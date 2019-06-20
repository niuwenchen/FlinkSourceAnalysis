package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/20.
 */
public interface TypeDeserializer<T> {
    TypeSerializer<T> duplicate();


    T deserialize(DataInputView source) throws IOException;
    T deserialize(T reuse, DataInputView source) throws IOException;
    int getLength();
    boolean canEqual(Object obj);

    boolean equals(Object obj);

    int hashCode();
}
