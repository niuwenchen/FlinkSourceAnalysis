package com.jackniu.flink.api.java.typeutils.runtime;

import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by JackNiu on 2019/6/27.
 */
public class DataOutputViewStream  extends OutputStream {
    protected DataOutputView outputView;

    public DataOutputViewStream(DataOutputView outputView){
        this.outputView = outputView;
    }

    @Override
    public void write(int b) throws IOException {
        outputView.writeByte(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputView.write(b, off, len);
    }
}
