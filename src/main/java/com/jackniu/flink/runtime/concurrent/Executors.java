package com.jackniu.flink.runtime.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class Executors {
    private static final Logger LOG = LoggerFactory.getLogger(Executors.class);

    /**
     * Return a direct executor. The direct executor directly executes the runnable in the calling
     * thread.
     *
     * @return Direct executor
     */
    public static Executor directExecutor() {
        return DirectExecutor.INSTANCE;
    }

    /**
     * Direct executor implementation.
     */
    private static class DirectExecutor implements Executor {

        static final DirectExecutor INSTANCE = new DirectExecutor();

        private DirectExecutor() {}

        @Override
        public void execute(@Nonnull Runnable command) {
            command.run();
        }
    }

    /**
     * Return a direct execution context. The direct execution context executes the runnable directly
     * in the calling thread.
     *
     * @return Direct execution context.
     */
    public static ExecutionContext directExecutionContext() {
        return DirectExecutionContext.INSTANCE;
    }

    /**
     * Direct execution context.
     */
    private static class DirectExecutionContext implements ExecutionContext {

        static final DirectExecutionContext INSTANCE = new DirectExecutionContext();

        private DirectExecutionContext() {}

        @Override
        public void execute(Runnable runnable) {
            runnable.run();
        }

        @Override
        public void reportFailure(Throwable cause) {
            throw new IllegalStateException("Error in direct execution context.", cause);
        }

        @Override
        public ExecutionContext prepare() {
            return this;
        }
    }

}
