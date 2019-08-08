package com.jackniu.flink.util;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface MutableObjectIterator<E> {

    /**
     * Gets the next element from the collection. The contents of that next element is put into the
     * given reuse object, if the type is mutable.
     *
     * @param reuse The target object into which to place next element if E is mutable.
     * @return The filled object or <code>null</code> if the iterator is exhausted.
     *
     * @throws IOException Thrown, if a problem occurred in the underlying I/O layer or in the
     *                     serialization / deserialization logic
     */
    E next(E reuse) throws IOException;

    /**
     * Gets the next element from the collection. The iterator implementation
     * must obtain a new instance.
     *
     * @return The object or <code>null</code> if the iterator is exhausted.
     *
     * @throws IOException Thrown, if a problem occurred in the underlying I/O layer or in the
     *                     serialization / deserialization logic
     */
    E next() throws IOException;

}
