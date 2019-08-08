package com.jackniu.flink.runtime.state;

import com.jackniu.flink.util.StringBasedID;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class StateHandleID extends StringBasedID {

    private static final long serialVersionUID = 1L;

    public StateHandleID(String keyString) {
        super(keyString);
    }
}
