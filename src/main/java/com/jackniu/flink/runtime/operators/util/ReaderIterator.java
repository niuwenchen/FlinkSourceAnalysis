package com.jackniu.flink.runtime.operators.util;

import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.runtime.io.network.api.reader.MutableReader;
import com.jackniu.flink.runtime.plugable.DeserializationDelegate;
import com.jackniu.flink.runtime.plugable.NonReusingDeserializationDelegate;
import com.jackniu.flink.runtime.plugable.ReusingDeserializationDelegate;
import com.jackniu.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class ReaderIterator<T> implements MutableObjectIterator<T> {

    private final MutableReader<DeserializationDelegate<T>> reader;   // the source

    private final ReusingDeserializationDelegate<T> reusingDelegate;
    private final NonReusingDeserializationDelegate<T> nonReusingDelegate;

    /**
     * Creates a new iterator, wrapping the given reader.
     *
     * @param reader The reader to wrap.
     */
    public ReaderIterator(MutableReader<DeserializationDelegate<T>> reader, TypeSerializer<T> serializer) {
        this.reader = reader;
        this.reusingDelegate = new ReusingDeserializationDelegate<T>(serializer);
        this.nonReusingDelegate = new NonReusingDeserializationDelegate<T>(serializer);
    }

    @Override
    public T next(T target) throws IOException {
        this.reusingDelegate.setInstance(target);
        try {
            if (this.reader.next(this.reusingDelegate)) {
                return this.reusingDelegate.getInstance();
            } else {
                return null;
            }
        }
        catch (InterruptedException e) {
            throw new IOException("Reader interrupted.", e);
        }
    }

    @Override
    public T next() throws IOException {
        try {
            if (this.reader.next(this.nonReusingDelegate)) {
                return this.nonReusingDelegate.getInstance();
            } else {
                return null;
            }
        }
        catch (InterruptedException e) {
            throw new IOException("Reader interrupted.", e);
        }
    }
}

