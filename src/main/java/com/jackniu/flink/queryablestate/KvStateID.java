package com.jackniu.flink.queryablestate;

import com.jackniu.flink.util.AbstractID;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class KvStateID  extends AbstractID{
    private static final long serialVersionUID = 1L;

    public KvStateID() {
        super();
    }

    public KvStateID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }
}
