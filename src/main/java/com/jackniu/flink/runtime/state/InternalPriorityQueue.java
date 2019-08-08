package com.jackniu.flink.runtime.state;

import com.jackniu.flink.util.CloseableIterator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface InternalPriorityQueue<T> {
    @Nullable
    T poll();
    @Nullable
    T peek();


    boolean add(@Nonnull T toAdd);
    boolean remove(@Nonnull T toRemove);
    boolean isEmpty();
    @Nonnegative
    int size();

    /**
     * Adds all the given elements to the set.
     */
    void addAll(@Nullable Collection<? extends T> toAdd);

    /**
     * Iterator over all elements, no order guaranteed. Iterator must be closed after usage.
     */
    @Nonnull
    CloseableIterator<T> iterator();


}
