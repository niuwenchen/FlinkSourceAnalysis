package com.jackniu.flink.runtime.io.disk.iomanager;

import com.jackniu.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class SynchronousBufferFileReader extends SynchronousFileIOChannel implements BufferFileReader {

    private final BufferFileChannelReader reader;

    private boolean hasReachedEndOfFile;

    public SynchronousBufferFileReader(ID channelID, boolean writeEnabled) throws IOException {
        super(channelID, writeEnabled);
        this.reader = new BufferFileChannelReader(fileChannel);
    }

    @Override
    public void readInto(Buffer buffer) throws IOException {
        if (fileChannel.size() - fileChannel.position() > 0) {
            hasReachedEndOfFile = reader.readBufferFromFileChannel(buffer);
        }
        else {
            buffer.recycleBuffer();
        }
    }

    @Override
    public void seekToPosition(long position) throws IOException {
        fileChannel.position(position);
    }

    @Override
    public boolean hasReachedEndOfFile() {
        return hasReachedEndOfFile;
    }
}

