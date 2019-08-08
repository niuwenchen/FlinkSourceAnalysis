package com.jackniu.flink.core.memory;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface SeekableDataOutputView extends DataOutputView {

    /**
     * Sets the write pointer to the given position.
     *
     * @param position The new write position.
     */
    void setWritePosition(long position);
}
