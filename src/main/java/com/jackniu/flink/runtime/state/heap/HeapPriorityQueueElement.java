package com.jackniu.flink.runtime.state.heap;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface HeapPriorityQueueElement {
    /**
     * The index that indicates that a {@link HeapPriorityQueueElement} object is not contained in and managed by any
     * {@link HeapPriorityQueue}. We do not strictly enforce that internal indexes must be reset to this value when
     * elements are removed from a {@link HeapPriorityQueue}.
     */
    int NOT_CONTAINED = Integer.MIN_VALUE;

    /**
     * Returns the current index of this object in the internal array of {@link HeapPriorityQueue}.
     */
    int getInternalIndex();

    /**
     * Sets the current index of this object in the {@link HeapPriorityQueue} and should only be called by the owning
     * {@link HeapPriorityQueue}.
     *
     * @param newIndex the new index in the timer heap.
     */
    void setInternalIndex(int newIndex);

}
