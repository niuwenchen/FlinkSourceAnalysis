package com.jackniu.flink.api.common.accumulators;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface Accumulator<V, R extends Serializable> extends Serializable, Cloneable {
    /**
     * @param value
     *            The value to add to the accumulator object
     */
    void add(V value);

    /**
     * @return local The local value from the current UDF context
     */
    R getLocalValue();

    /**
     * Reset the local value. This only affects the current UDF context.
     */
    void resetLocal();

    /**
     * Used by system internally to merge the collected parts of an accumulator
     * at the end of the job.
     *
     * @param other Reference to accumulator to merge in.
     */
    void merge(Accumulator<V, R> other);

    /**
     * Duplicates the accumulator. All subclasses need to properly implement
     * cloning and cannot throw a {@link java.lang.CloneNotSupportedException}
     *
     * @return The duplicated accumulator.
     */
    Accumulator<V, R> clone();
}
