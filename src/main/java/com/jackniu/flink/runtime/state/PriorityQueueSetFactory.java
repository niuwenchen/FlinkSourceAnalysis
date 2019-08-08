package com.jackniu.flink.runtime.state;

import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.runtime.state.heap.HeapPriorityQueueElement;

import javax.annotation.Nonnull;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface PriorityQueueSetFactory {
    /**
     * Creates a {@link KeyGroupedInternalPriorityQueue}.
     *
     * @param stateName                    unique name for associated with this queue.
     * @param byteOrderedElementSerializer a serializer that with a format that is lexicographically ordered in
     *                                     alignment with elementPriorityComparator.
     * @param <T>                          type of the stored elements.
     * @return the queue with the specified unique name.
     */
    @Nonnull
    <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(
            @Nonnull String stateName,
            @Nonnull TypeSerializer<T> byteOrderedElementSerializer);
}
