package com.jackniu.flink.core.memory;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/18.
 */
public interface DataOutputView extends DataOutput {
    void skipBytesToWrite(int numByte) throws IOException;
    void write(DataInputView source, int numBytes) throws IOException;
}
