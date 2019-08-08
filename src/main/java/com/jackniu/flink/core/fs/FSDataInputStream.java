package com.jackniu.flink.core.fs;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by JackNiu on 2019/7/5.
 */
public abstract class FSDataInputStream  extends InputStream {
    public abstract void seek(long desired) throws IOException;
    /**
     * Gets the current position in the input stream.
     *
     * @return current position in the input stream
     * @throws IOException Thrown if an I/O error occurred in the underlying stream
     *                     implementation while accessing the stream's position.
     */
    public abstract long getPos() throws IOException;
}
