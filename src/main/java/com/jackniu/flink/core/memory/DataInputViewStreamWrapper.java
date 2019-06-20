package com.jackniu.flink.core.memory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class DataInputViewStreamWrapper extends DataInputStream implements DataInputView {
    public DataInputViewStreamWrapper(InputStream in) {
        super(in);
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        if (skipBytes(numBytes) != numBytes){
            throw new EOFException("Could not skip " + numBytes + " bytes.");
        }
    }
}
