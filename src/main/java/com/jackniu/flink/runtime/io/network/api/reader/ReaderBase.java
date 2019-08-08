package com.jackniu.flink.runtime.io.network.api.reader;

import com.jackniu.flink.runtime.event.TaskEvent;
import com.jackniu.flink.runtime.util.event.EventListener;


import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ReaderBase {
    /**
     * Returns whether the reader has consumed the input.
     */
    boolean isFinished();

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    void sendTaskEvent(TaskEvent event) throws IOException;

    void registerTaskEventListener(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType);

    // ------------------------------------------------------------------------
    // Iterations
    // ------------------------------------------------------------------------

    void setIterativeReader();

    void startNextSuperstep();

    boolean hasReachedEndOfSuperstep();

}
