package com.jackniu.flink.runtime.operators.util;

import com.jackniu.flink.util.MutableObjectIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface CloseableInputProvider<E> extends Closeable
{
    /**
     * Gets the iterator over this input.
     *
     * @return The iterator provided by this iterator provider.
     * @throws InterruptedException
     */
    public MutableObjectIterator<E> getIterator() throws InterruptedException, IOException;
}
