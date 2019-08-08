package com.jackniu.flink.util.function;

import com.jackniu.flink.util.ExceptionUtils;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface ThrowingRunnable<E extends Throwable> {

    /**
     * The work method.
     *
     * @throws E Exceptions may be thrown.
     */
    void run() throws E;

    /**
     * Converts a {@link ThrowingRunnable} into a {@link Runnable} which throws all checked exceptions
     * as unchecked.
     *
     * @param throwingRunnable to convert into a {@link Runnable}
     * @return {@link Runnable} which throws all checked exceptions as unchecked.
     */
    static Runnable unchecked(ThrowingRunnable<?> throwingRunnable) {
        return () -> {
            try {
                throwingRunnable.run();
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            }
        };
    }
}
