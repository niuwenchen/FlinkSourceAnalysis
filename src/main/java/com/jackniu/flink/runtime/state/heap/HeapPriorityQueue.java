package com.jackniu.flink.runtime.state.heap;

import com.jackniu.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Created by JackNiu on 2019/7/5.
 */
public class HeapPriorityQueue<T extends HeapPriorityQueueElement>
        extends AbstractHeapPriorityQueue<T> {
    private static final int QUEUE_HEAD_INDEX = 1;
    @Nonnull
    protected final PriorityComparator<T> elementPriorityComparator;
    /**
     * Creates an empty {@link HeapPriorityQueue} with the requested initial capacity.
     *
     * @param elementPriorityComparator comparator for the priority of contained elements.
     * @param minimumCapacity the minimum and initial capacity of this priority queue.
     */
    @SuppressWarnings("unchecked")
    public HeapPriorityQueue(
            @Nonnull PriorityComparator<T> elementPriorityComparator,
            @Nonnegative int minimumCapacity) {
        super(minimumCapacity);
        this.elementPriorityComparator = elementPriorityComparator;
    }

    public void adjustModifiedElement(@Nonnull T element) {
        final int elementIndex = element.getInternalIndex();
        if (element == queue[elementIndex]) {
            adjustElementAtIndex(element, elementIndex);
        }
    }

    @Override
    protected int getHeadElementIndex() {
        return QUEUE_HEAD_INDEX;
    }

    @Override
    protected void addInternal(@Nonnull T element) {
        final int newSize = increaseSizeByOne();
        moveElementToIdx(element, newSize);
        siftUp(newSize);
    }

    @Override
    protected T removeInternal(int removeIdx) {
        T[] heap = this.queue;
        T removedValue = heap[removeIdx];

        assert removedValue.getInternalIndex() == removeIdx;

        final int oldSize = size;

        if (removeIdx != oldSize) {
            T element = heap[oldSize];
            moveElementToIdx(element, removeIdx);
            adjustElementAtIndex(element, removeIdx);
        }

        heap[oldSize] = null;

        --size;
        return removedValue;
    }

    private void adjustElementAtIndex(T element, int index) {
        siftDown(index);
        if (queue[index] == element) {
            siftUp(index);
        }
    }

    private void siftUp(int idx) {
        final T[] heap = this.queue;
        final T currentElement = heap[idx];
        int parentIdx = idx >>> 1;

        while (parentIdx > 0 && isElementPriorityLessThen(currentElement, heap[parentIdx])) {
            moveElementToIdx(heap[parentIdx], idx);
            idx = parentIdx;
            parentIdx >>>= 1;
        }

        moveElementToIdx(currentElement, idx);
    }

    private void siftDown(int idx) {
        final T[] heap = this.queue;
        final int heapSize = this.size;

        final T currentElement = heap[idx];
        int firstChildIdx = idx << 1;
        int secondChildIdx = firstChildIdx + 1;

        if (isElementIndexValid(secondChildIdx, heapSize) &&
                isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
            firstChildIdx = secondChildIdx;
        }

        while (isElementIndexValid(firstChildIdx, heapSize) &&
                isElementPriorityLessThen(heap[firstChildIdx], currentElement)) {
            moveElementToIdx(heap[firstChildIdx], idx);
            idx = firstChildIdx;
            firstChildIdx = idx << 1;
            secondChildIdx = firstChildIdx + 1;

            if (isElementIndexValid(secondChildIdx, heapSize) &&
                    isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
                firstChildIdx = secondChildIdx;
            }
        }

        moveElementToIdx(currentElement, idx);
    }

    private boolean isElementIndexValid(int elementIndex, int heapSize) {
        return elementIndex <= heapSize;
    }

    private boolean isElementPriorityLessThen(T a, T b) {
        return elementPriorityComparator.comparePriority(a, b) < 0;
    }

    private int increaseSizeByOne() {
        final int oldArraySize = queue.length;
        final int minRequiredNewSize = ++size;
        if (minRequiredNewSize >= oldArraySize) {
            final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1;
            resizeQueueArray(oldArraySize + grow, minRequiredNewSize);
        }
        // TODO implement shrinking as well?
        return minRequiredNewSize;
    }
}
