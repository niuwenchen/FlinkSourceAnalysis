package com.jackniu.flink.runtime.util;

import com.jackniu.flink.util.MutableObjectIterator;

/**
 * Created by JackNiu on 2019/7/8.
 */
public final class EmptyMutableObjectIterator<E> implements MutableObjectIterator<E> {

    /**
     * The singleton instance.
     */
    private static final EmptyMutableObjectIterator<Object> INSTANCE = new EmptyMutableObjectIterator<Object>();

    /**
     * Gets a singleton instance of the empty iterator.
     *
     * @param <E> The type of the objects (not) returned by the iterator.
     * @return An instance of the iterator.
     */
    public static <E> MutableObjectIterator<E> get() {
        @SuppressWarnings("unchecked")
        MutableObjectIterator<E> iter = (MutableObjectIterator<E>) INSTANCE;
        return iter;
    }

    /**
     * Always returns null.
     *
     * @see MutableObjectIterator#next(Object)
     */
    @Override
    public E next(E target) {
        return null;
    }

    /**
     * Always returns null.
     *
     * @see MutableObjectIterator#next()
     */
    @Override
    public E next() {
        return null;
    }
}
