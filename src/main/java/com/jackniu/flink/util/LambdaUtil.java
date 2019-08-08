package com.jackniu.flink.util;

import com.jackniu.flink.util.function.SupplierWithException;
import com.jackniu.flink.util.function.ThrowingConsumer;
import com.jackniu.flink.util.function.ThrowingRunnable;

/**
 * Created by JackNiu on 2019/7/6.
 */
public final class LambdaUtil {
    private LambdaUtil() {
        throw new AssertionError();
    }

    /**
     * This method supplies all elements from the input to the consumer. Exceptions that happen on elements are
     * suppressed until all elements are processed. If exceptions happened for one or more of the inputs, they are
     * reported in a combining suppressed exception.
     *
     * @param inputs iterator for all inputs to the throwingConsumer.
     * @param throwingConsumer this consumer will be called for all elements delivered by the input iterator.
     * @param <T> the type of input.
     * @throws Exception collected exceptions that happened during the invocation of the consumer on the input elements.
     */
    public static <T> void applyToAllWhileSuppressingExceptions(
            Iterable<T> inputs,
            ThrowingConsumer<T, ? extends Exception> throwingConsumer) throws Exception {

        if (inputs != null && throwingConsumer != null) {
            Exception exception = null;

            for (T input : inputs) {

                if (input != null) {
                    try {
                        throwingConsumer.accept(input);
                    } catch (Exception ex) {
                        exception = ExceptionUtils.firstOrSuppressed(ex, exception);
                    }
                }
            }

            if (exception != null) {
                throw exception;
            }
        }
    }

    /**
     * Runs the given runnable with the given ClassLoader as the thread's
     * {@link Thread#setContextClassLoader(ClassLoader) context class loader}.
     *
     * <p>The method will make sure to set the context class loader of the calling thread
     * back to what it was before after the runnable completed.
     */
    public static <E extends Throwable> void withContextClassLoader(
            final ClassLoader cl,
            final ThrowingRunnable<E> r) throws E {

        final Thread currentThread = Thread.currentThread();
        final ClassLoader oldClassLoader = currentThread.getContextClassLoader();

        try {
            currentThread.setContextClassLoader(cl);
            r.run();
        }
        finally {
            currentThread.setContextClassLoader(oldClassLoader);
        }
    }

    /**
     * Runs the given runnable with the given ClassLoader as the thread's
     * {@link Thread#setContextClassLoader(ClassLoader) context class loader}.
     *
     * <p>The method will make sure to set the context class loader of the calling thread
     * back to what it was before after the runnable completed.
     */
    public static <R, E extends Throwable> R withContextClassLoader(
            final ClassLoader cl,
            final SupplierWithException<R, E> s) throws E {

        final Thread currentThread = Thread.currentThread();
        final ClassLoader oldClassLoader = currentThread.getContextClassLoader();

        try {
            currentThread.setContextClassLoader(cl);
            return s.get();
        }
        finally {
            currentThread.setContextClassLoader(oldClassLoader);
        }
    }
}
