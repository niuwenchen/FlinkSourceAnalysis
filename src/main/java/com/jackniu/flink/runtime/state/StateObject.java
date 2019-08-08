package com.jackniu.flink.runtime.state;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface StateObject extends Serializable {
    /**
     * Discards the state referred to and solemnly owned by this handle, to free up resources in
     * the persistent storage. This method is called when the state represented by this
     * object will not be used any more.
     */
    void discardState() throws Exception;
    long getStateSize();

}
