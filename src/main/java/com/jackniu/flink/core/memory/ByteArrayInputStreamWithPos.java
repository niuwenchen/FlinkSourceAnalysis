package com.jackniu.flink.core.memory;

import com.jackniu.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class ByteArrayInputStreamWithPos extends InputStream {
    private static final byte[] EMPTY = new byte[0];

    protected byte[] buffer;
    protected int position;
    protected int count;
    protected int mark = 0;

    public ByteArrayInputStreamWithPos() {
        this(EMPTY);
    }

    public ByteArrayInputStreamWithPos(byte[] buffer) {
        this(buffer, 0, buffer.length);
    }

    public ByteArrayInputStreamWithPos(byte[] buffer, int offset, int length) {
        setBuffer(buffer, offset, length);
    }

    @Override
    public int read() {
        return (position < count) ? 0xFF & (buffer[position++]) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        Preconditions.checkNotNull(b);

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        if (position >= count) {
            return -1; // signal EOF
        }

        int available = count - position;

        if (len > available) {
            len = available;
        }

        if (len <= 0) {
            return 0;
        }

        System.arraycopy(buffer, position, b, off, len);
        position += len;
        return len;
    }

    @Override
    public long skip(long toSkip) {
        long remain = count - position;

        if (toSkip < remain) {
            remain = toSkip < 0 ? 0 : toSkip;
        }

        position += remain;
        return remain;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readAheadLimit) {
        mark = position;
    }

    @Override
    public void reset() {
        position = mark;
    }

    @Override
    public int available() {
        return count - position;
    }

    @Override
    public void close() throws IOException {
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int pos) {
        Preconditions.checkArgument(pos >= 0 && pos <= count, "Position out of bounds.");
        this.position = pos;
    }

    public void setBuffer(byte[] buffer, int offset, int length) {
        this.count = Math.min(buffer.length, offset + length);
        setPosition(offset);
        this.buffer = buffer;
        this.mark = offset;
    }
}
