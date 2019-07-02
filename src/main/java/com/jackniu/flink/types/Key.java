package com.jackniu.flink.types;

/**
 * Created by JackNiu on 2019/6/19.
 */
public interface Key<T> extends Value, Comparable<T> {
    public int hashCode();

    /**
     * Compares the object on equality with another object.
     *
     * @param other The other object to compare against.
     *
     * @return True, iff this object is identical to the other object, false otherwise.
     */
    public boolean equals(Object other);

}
