package com.jackniu.flink.runtime.state;

import com.jackniu.flink.annotations.VisibleForTesting;
import com.jackniu.flink.util.StringBasedID;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class SharedStateRegistryKey extends StringBasedID {

    private static final long serialVersionUID = 1L;

    public SharedStateRegistryKey(String prefix, StateHandleID stateHandleID) {
        super(prefix + '-' + stateHandleID);
    }

    @VisibleForTesting
    public SharedStateRegistryKey(String keyString) {
        super(keyString);
    }
}
