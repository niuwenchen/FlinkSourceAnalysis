package com.jackniu.flink.runtime.state;

import com.jackniu.flink.core.fs.FSDataInputStream;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface StreamStateHandle extends StateObject {
    /**
     * Returns an {@link FSDataInputStream} that can be used to read back the data that
     * was previously written to the stream.
     */
    FSDataInputStream openInputStream() throws IOException;
}
