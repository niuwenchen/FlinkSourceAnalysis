package com.jackniu.flink.core.io;

/**
 * Created by JackNiu on 2019/6/19.
 */

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * This interface must be implemented by every class whose objects have to be serialized to their binary representation
 * and vice-versa. In particular, records have to implement this interface in order to specify how their data can be
 * transferred to a binary representation.
 *
 * <p>When implementing this Interface make sure that the implementing class has a default
 * (zero-argument) constructor!
 */


public interface IOReadableWritable {
    void write(DataOutputView out) throws IOException;
    void read(DataInputView in) throws IOException;

}
