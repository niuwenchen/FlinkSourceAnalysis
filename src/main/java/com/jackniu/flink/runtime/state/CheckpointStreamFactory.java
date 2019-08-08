package com.jackniu.flink.runtime.state;

import com.jackniu.flink.core.fs.FSDataOutputStream;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface CheckpointStreamFactory {
    CheckpointStateOutputStream createCheckpointStateOutputStream(CheckpointedStateScope scope) throws IOException;
    abstract class CheckpointStateOutputStream extends FSDataOutputStream {
        @Nullable
        public abstract StreamStateHandle closeAndGetHandle() throws IOException;
        @Override
        public abstract void close() throws IOException;
    }
}
