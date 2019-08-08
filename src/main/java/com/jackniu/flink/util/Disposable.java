package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface Disposable {
    /**
     * Disposes the object and releases all resources. After calling this method, calling any methods on the
     * object may result in undefined behavior.
     *
     * @throws Exception if something goes wrong during disposal.
     */
    void dispose() throws Exception;

}
