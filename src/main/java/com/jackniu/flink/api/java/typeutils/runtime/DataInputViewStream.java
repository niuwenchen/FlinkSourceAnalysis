package com.jackniu.flink.api.java.typeutils.runtime;

import com.jackniu.flink.core.memory.DataInputView;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by JackNiu on 2019/6/27.
 */
public class DataInputViewStream extends InputStream {
    protected DataInputView inputView;

    public DataInputViewStream(DataInputView inputView) {
        this.inputView = inputView;
    }

    public DataInputView getInputView(){
        return inputView;
    }

    @Override
    public int read() throws IOException {
        try {
            return inputView.readUnsignedByte();
        } catch(EOFException ex) {
            return -1;
        }
    }
    @Override
    public long skip(long n) throws IOException {
        long counter = n;
        while(counter > Integer.MAX_VALUE) {
            int skippedBytes = inputView.skipBytes(Integer.MAX_VALUE);

            if (skippedBytes == 0) {
                return n - counter;
            }

            counter -= skippedBytes;
        }
        return n - counter - inputView.skipBytes((int) counter);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inputView.read(b, off, len);
    }


}
