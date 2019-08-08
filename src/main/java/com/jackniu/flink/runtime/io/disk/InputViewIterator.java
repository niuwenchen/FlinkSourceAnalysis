package com.jackniu.flink.runtime.io.disk;

import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class InputViewIterator<E> implements MutableObjectIterator<E>
{
    private DataInputView inputView;

    private final TypeSerializer<E> serializer;

    public InputViewIterator(DataInputView inputView, TypeSerializer<E> serializer) {
        this.inputView = inputView;
        this.serializer = serializer;
    }

    @Override
    public E next(E reuse) throws IOException {
        try {
            return this.serializer.deserialize(reuse, this.inputView);
        } catch (EOFException e) {
            return null;
        }
    }

    @Override
    public E next() throws IOException {
        try {
            return this.serializer.deserialize(this.inputView);
        } catch (EOFException e) {
            return null;
        }
    }
}
