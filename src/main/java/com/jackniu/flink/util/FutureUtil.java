package com.jackniu.flink.util;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class FutureUtil {
    private FutureUtil() {
        throw new AssertionError();
    }

    public static <T> T runIfNotDoneAndGet(RunnableFuture<T> future) throws ExecutionException, InterruptedException {

        if (null == future) {
            return null;
        }

        if (!future.isDone()) {
            future.run();
        }

        return future.get();
    }

    public static void waitForAll(long timeoutMillis, Future<?>...futures) throws Exception {
        waitForAll(timeoutMillis, Arrays.asList(futures));
    }

    public static void waitForAll(long timeoutMillis, Collection<Future<?>> futures) throws Exception {
        long startMillis = System.currentTimeMillis();
        Set<Future<?>> futuresSet = new HashSet<>();
        futuresSet.addAll(futures);

        while (System.currentTimeMillis() < startMillis + timeoutMillis) {
            if (futuresSet.isEmpty()) {
                return;
            }
            Iterator<Future<?>> futureIterator = futuresSet.iterator();
            while (futureIterator.hasNext()) {
                Future<?> future = futureIterator.next();
                if (future.isDone()) {
                    future.get();
                    futureIterator.remove();
                }
            }

            Thread.sleep(10);
        }

        if (!futuresSet.isEmpty()) {
            throw new TimeoutException(String.format("Some of the futures have not finished [%s]", futuresSet));
        }
    }
}
