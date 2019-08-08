package com.jackniu.flink.util;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ResettableMutableObjectIterator<E> extends MutableObjectIterator<E> {

    /**
     * Resets the iterator.
     *
     * @throws IOException May be thrown when the serialization into buffers or the spilling to secondary
     *                     storage fails.
     */
    public void reset() throws IOException;

}