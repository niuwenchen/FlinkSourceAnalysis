package com.jackniu.flink.core.memory;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface SeekableDataInputView extends DataInputView {

    /**
     * Sets the read pointer to the given position.
     *
     * @param position The new read position.
     */
    void setReadPosition(long position);
}
