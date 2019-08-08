package com.jackniu.flink.util.function;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface ThrowingConsumer<T, E extends Throwable> {
    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws E on errors during consumption
     */
    void accept(T t) throws E;
}
