package com.jackniu.flink.util.function;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface SupplierWithException<R, E extends Throwable> {

    /**
     * Gets the result of this supplier.
     *
     * @return The result of thus supplier.
     * @throws E This function may throw an exception.
     */
    R get() throws E;
}