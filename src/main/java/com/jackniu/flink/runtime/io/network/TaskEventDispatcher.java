package com.jackniu.flink.runtime.io.network;

import com.jackniu.flink.runtime.event.TaskEvent;
import com.jackniu.flink.runtime.io.network.api.TaskEventHandler;
import com.jackniu.flink.runtime.io.network.partition.ResultPartitionID;

import com.jackniu.flink.runtime.util.event.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class TaskEventDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(TaskEventDispatcher.class);

    private final Map<ResultPartitionID, TaskEventHandler> registeredHandlers = new HashMap<>();

    /**
     * Registers the given partition for incoming task events allowing calls to {@link
     * #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId
     * 		the partition ID
     */
    public void registerPartition(ResultPartitionID partitionId) {
        checkNotNull(partitionId);

        synchronized (registeredHandlers) {
            LOG.debug("registering {}", partitionId);
            if (registeredHandlers.put(partitionId, new TaskEventHandler()) != null) {
                throw new IllegalStateException(
                        "Partition " + partitionId + " already registered at task event dispatcher.");
            }
        }
    }

    /**
     * Removes the given partition from listening to incoming task events, thus forbidding calls to
     * {@link #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId
     * 		the partition ID
     */
    public void unregisterPartition(ResultPartitionID partitionId) {
        checkNotNull(partitionId);

        synchronized (registeredHandlers) {
            LOG.debug("unregistering {}", partitionId);
            // NOTE: tolerate un-registration of non-registered task (unregister is always called
            //       in the cleanup phase of a task even if it never came to the registration - see
            //       Task.java)
            registeredHandlers.remove(partitionId);
        }
    }

    /**
     * Subscribes a listener to this dispatcher for events on a partition.
     *
     * @param partitionId
     * 		ID of the partition to subscribe for (must be registered via {@link
     * 		#registerPartition(ResultPartitionID)} first!)
     * @param eventListener
     * 		the event listener to subscribe
     * @param eventType
     * 		event type to subscribe to
     */
    public void subscribeToEvent(
            ResultPartitionID partitionId,
            EventListener<TaskEvent> eventListener,
            Class<? extends TaskEvent> eventType) {
        checkNotNull(partitionId);
        checkNotNull(eventListener);
        checkNotNull(eventType);

        TaskEventHandler taskEventHandler;
        synchronized (registeredHandlers) {
            taskEventHandler = registeredHandlers.get(partitionId);
        }
        if (taskEventHandler == null) {
            throw new IllegalStateException(
                    "Partition " + partitionId + " not registered at task event dispatcher.");
        }
        taskEventHandler.subscribe(eventListener, eventType);
    }

    /**
     * Publishes the event to the registered {@link EventListener} instances.
     *

     *
     * @return whether the event was published to a registered event handler (initiated via {@link
     * #registerPartition(ResultPartitionID)}) or not
     */
    public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
        checkNotNull(partitionId);
        checkNotNull(event);

        TaskEventHandler taskEventHandler;
        synchronized (registeredHandlers) {
            taskEventHandler = registeredHandlers.get(partitionId);
        }

        if (taskEventHandler != null) {
            taskEventHandler.publish(event);
            return true;
        }

        return false;
    }



    /**
     * Removes all registered event handlers.
     */
    public void clearAll() {
        synchronized (registeredHandlers) {
            registeredHandlers.clear();
        }
    }

}
