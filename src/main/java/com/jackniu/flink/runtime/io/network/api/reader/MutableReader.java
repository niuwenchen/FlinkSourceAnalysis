package com.jackniu.flink.runtime.io.network.api.reader;

import com.jackniu.flink.core.io.IOReadableWritable;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface MutableReader<T extends IOReadableWritable> extends ReaderBase {

    boolean next(T target) throws IOException, InterruptedException;

    void clearBuffers();
}
