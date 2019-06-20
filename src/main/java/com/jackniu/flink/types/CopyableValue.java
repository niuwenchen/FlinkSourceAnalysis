package com.jackniu.flink.types;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/19.
 */
public interface CopyableValue<T> extends Value {
    int getBinaryLength();
    void copyTo(T target);
    T copy();
    void copy(DataInputView source, DataOutputView target) throws IOException;

}
