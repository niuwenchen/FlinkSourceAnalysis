package com.jackniu.flink.runtime.io.disk.iomanager;

import com.jackniu.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Created by JackNiu on 2019/7/7.
 */
public abstract class AbstractFileIOChannel implements FileIOChannel {

    /** Logger object for channel and its subclasses */
    protected static final Logger LOG = LoggerFactory.getLogger(FileIOChannel.class);

    /** The ID of the underlying channel. */
    protected final FileIOChannel.ID id;

    /** A file channel for NIO access to the file. */
    protected final FileChannel fileChannel;


    /**
     * Creates a new channel to the path indicated by the given ID. The channel hands IO requests to
     * the given request queue to be processed.
     *
     * @param channelID The id describing the path of the file that the channel accessed.
     * @param writeEnabled Flag describing whether the channel should be opened in read/write mode, rather
     *                     than in read-only mode.
     * @throws IOException Thrown, if the channel could no be opened.
     */
    protected AbstractFileIOChannel(FileIOChannel.ID channelID, boolean writeEnabled) throws IOException {
        this.id = Preconditions.checkNotNull(channelID);

        try {
            @SuppressWarnings("resource")
            RandomAccessFile file = new RandomAccessFile(id.getPath(), writeEnabled ? "rw" : "r");
            this.fileChannel = file.getChannel();
        }
        catch (IOException e) {
            throw new IOException("Channel to path '" + channelID.getPath() + "' could not be opened.", e);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public final FileIOChannel.ID getChannelID() {
        return this.id;
    }

    @Override
    public long getSize() throws IOException {
        FileChannel channel = fileChannel;
        return channel == null ? 0 : channel.size();
    }

    @Override
    public abstract boolean isClosed();

    @Override
    public abstract void close() throws IOException;

    @Override
    public void deleteChannel() {
        if (!isClosed() || this.fileChannel.isOpen()) {
            throw new IllegalStateException("Cannot delete a channel that is open.");
        }

        // make a best effort to delete the file. Don't report exceptions.
        try {
            File f = new File(this.id.getPath());
            if (f.exists()) {
                f.delete();
            }
        } catch (Throwable t) {}
    }

    @Override
    public void closeAndDelete() throws IOException {
        try {
            close();
        } finally {
            deleteChannel();
        }
    }

    @Override
    public FileChannel getNioFileChannel() {
        return fileChannel;
    }
}
