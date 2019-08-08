package com.jackniu.flink.runtime.io.disk.iomanager;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public abstract class SynchronousFileIOChannel extends AbstractFileIOChannel {

    protected SynchronousFileIOChannel(FileIOChannel.ID channelID, boolean writeEnabled) throws IOException {
        super(channelID, writeEnabled);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean isClosed() {
        return !this.fileChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        if (this.fileChannel.isOpen()) {
            this.fileChannel.close();
        }
    }
}
