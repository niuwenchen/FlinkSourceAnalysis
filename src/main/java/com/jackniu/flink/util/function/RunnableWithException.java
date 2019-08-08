package com.jackniu.flink.util.function;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface RunnableWithException extends ThrowingRunnable<Exception> {

    /**
     * The work method.
     *
     * @throws Exception Exceptions may be thrown.
     */
    @Override
    void run() throws Exception;
}