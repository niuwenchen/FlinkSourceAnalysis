package com.jackniu.flink.runtime.io.network.api.reader;

import com.jackniu.flink.runtime.event.AbstractEvent;
import com.jackniu.flink.runtime.event.TaskEvent;
import com.jackniu.flink.runtime.io.network.api.EndOfPartitionEvent;
import com.jackniu.flink.runtime.io.network.api.EndOfSuperstepEvent;
import com.jackniu.flink.runtime.io.network.api.TaskEventHandler;
import com.jackniu.flink.runtime.io.network.partition.consumer.InputGate;
import com.jackniu.flink.runtime.util.event.EventListener;


import java.io.IOException;

import static com.jackniu.flink.util.Preconditions.checkState;

/**
 * Created by JackNiu on 2019/7/7.
 */
public abstract class AbstractReader implements ReaderBase {

    /** The input gate to read from. */
    protected final InputGate inputGate;

    /** The task event handler to manage task event subscriptions. */
    private final TaskEventHandler taskEventHandler = new TaskEventHandler();

    /** Flag indicating whether this reader allows iteration events. */
    private boolean isIterative;

    /**
     * The current number of end of superstep events (reset for each superstep). A superstep is
     * finished after an end of superstep event has been received for each input channel.
     */
    private int currentNumberOfEndOfSuperstepEvents;

    protected AbstractReader(InputGate inputGate) {
        this.inputGate = inputGate;
    }

    @Override
    public boolean isFinished() {
        return inputGate.isFinished();
    }

    // ------------------------------------------------------------------------
    // Events
    // ------------------------------------------------------------------------

    @Override
    public void registerTaskEventListener(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType) {
        taskEventHandler.subscribe(listener, eventType);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {
        inputGate.sendTaskEvent(event);
    }

    /**
     * Handles the event and returns whether the reader reached an end-of-stream event (either the
     * end of the whole stream or the end of an superstep).
     */
    protected boolean handleEvent(AbstractEvent event) throws IOException {
        final Class<?> eventType = event.getClass();

        try {
            // ------------------------------------------------------------
            // Runtime events
            // ------------------------------------------------------------

            // This event is also checked at the (single) input gate to release the respective
            // channel, at which it was received.
            if (eventType == EndOfPartitionEvent.class) {
                return true;
            }
            else if (eventType == EndOfSuperstepEvent.class) {
                return incrementEndOfSuperstepEventAndCheck();
            }

            // ------------------------------------------------------------
            // Task events (user)
            // ------------------------------------------------------------
            else if (event instanceof TaskEvent) {
                taskEventHandler.publish((TaskEvent) event);

                return false;
            }
            else {
                throw new IllegalStateException("Received unexpected event of type " + eventType + " at reader.");
            }
        }
        catch (Throwable t) {
            throw new IOException("Error while handling event of type " + eventType + ": " + t.getMessage(), t);
        }
    }

    public void publish(TaskEvent event){
        taskEventHandler.publish(event);
    }

    // ------------------------------------------------------------------------
    // Iterations
    // ------------------------------------------------------------------------

    @Override
    public void setIterativeReader() {
        isIterative = true;
    }

    @Override
    public void startNextSuperstep() {
        checkState(isIterative, "Tried to start next superstep in a non-iterative reader.");
        checkState(currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels(), "Tried to start next superstep before reaching end of previous superstep.");

        currentNumberOfEndOfSuperstepEvents = 0;
    }

    @Override
    public boolean hasReachedEndOfSuperstep() {
        return isIterative && currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels();

    }

    private boolean incrementEndOfSuperstepEventAndCheck() {
        checkState(isIterative, "Tried to increment superstep count in a non-iterative reader.");
        checkState(currentNumberOfEndOfSuperstepEvents + 1 <= inputGate.getNumberOfInputChannels(), "Received too many (" + currentNumberOfEndOfSuperstepEvents + ") end of superstep events.");

        return ++currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels();
    }

}
