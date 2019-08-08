package com.jackniu.flink.runtime.state;

import com.jackniu.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/5.
 */
public abstract class CheckpointMetadataOutputStream  extends FSDataOutputStream {
    public abstract CompletedCheckpointStorageLocation closeAndFinalizeCheckpoint() throws IOException;
    @Override
    public abstract void close() throws IOException;

}
