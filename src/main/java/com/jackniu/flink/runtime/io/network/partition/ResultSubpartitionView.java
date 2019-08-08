package com.jackniu.flink.runtime.io.network.partition;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ResultSubpartitionView {
    @Nullable
    ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException, InterruptedException;

    void notifyDataAvailable();

    void releaseAllResources() throws IOException;

    void notifySubpartitionConsumed() throws IOException;

    boolean isReleased();

    Throwable getFailureCause();

    /**
     * Returns whether the next buffer is an event or not.
     */
    boolean nextBufferIsEvent();

    boolean isAvailable();
}
