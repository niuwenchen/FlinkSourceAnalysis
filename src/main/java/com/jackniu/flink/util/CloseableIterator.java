package com.jackniu.flink.util;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    CloseableIterator<?> EMPTY_INSTANCE = CloseableIterator.adapterForIterator(Collections.emptyIterator());

    @Nonnull
    static <T> CloseableIterator<T> adapterForIterator(@Nonnull Iterator<T> iterator) {
        return new IteratorAdapter<>(iterator);
    }

    @SuppressWarnings("unchecked")
    static <T> CloseableIterator<T> empty() {
        return (CloseableIterator<T>) EMPTY_INSTANCE;
    }

    /**
     * Adapter from {@link Iterator} to {@link CloseableIterator}. Does nothing on {@link #close()}.
     *
     * @param <E> the type of iterated elements.
     */
    final class IteratorAdapter<E> implements CloseableIterator<E> {

        @Nonnull
        private final Iterator<E> delegate;

        IteratorAdapter(@Nonnull Iterator<E> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            delegate.forEachRemaining(action);
        }

        @Override
        public void close() {
        }
    }
}
