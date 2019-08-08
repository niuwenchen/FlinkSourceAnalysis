package com.jackniu.flink.core.fs;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by JackNiu on 2019/7/5.
 */
public abstract class FSDataOutputStream extends OutputStream
{
    public abstract long getPos() throws IOException;
    public abstract void flush() throws IOException;
    public abstract void sync() throws IOException;
    public abstract void close() throws IOException;
}
