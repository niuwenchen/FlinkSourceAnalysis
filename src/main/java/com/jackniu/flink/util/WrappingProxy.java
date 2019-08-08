package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/7/7.
 */
/**
 * Interface for objects that wrap another object and proxy (possibly a subset) of
 * the methods of that object.
 *
 * @param <T> The type that is wrapped.
 */

public interface WrappingProxy<T> {
    T getWrappedDelegate();

}
