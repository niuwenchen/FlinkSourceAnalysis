package com.jackniu.flink.util.function;

import com.jackniu.flink.util.FlinkException;

import java.util.function.Supplier;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface CheckedSupplier<R> extends SupplierWithException<R, Exception> {

    static <R> Supplier<R> unchecked(CheckedSupplier<R> checkedSupplier) {
        return () -> {
            try {
                return checkedSupplier.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    static <R> CheckedSupplier<R> checked(Supplier<R> supplier) {
        return () -> {
            try {
                return supplier.get();
            }
            catch (RuntimeException e) {
                throw new FlinkException(e);
            }
        };
    }
}

