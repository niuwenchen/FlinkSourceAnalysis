package com.jackniu.flink.runtime.state;

import java.util.concurrent.Executor;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface SharedStateRegistryFactory {
    /**
     * Factory method for {@link SharedStateRegistry}.
     *
     * @param deleteExecutor executor used to run (async) deletes.
     * @return a SharedStateRegistry object
     */
    SharedStateRegistry create(Executor deleteExecutor);
}
