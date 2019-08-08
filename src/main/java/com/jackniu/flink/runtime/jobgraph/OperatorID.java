package com.jackniu.flink.runtime.jobgraph;

import com.jackniu.flink.util.AbstractID;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class OperatorID extends AbstractID {
    private static final long serialVersionUID = 1L;

    public OperatorID() {
        super();
    }

    public OperatorID(byte[] bytes) {
        super(bytes);
    }

    public OperatorID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    public static OperatorID fromJobVertexID(JobVertexID id) {
        return new OperatorID(id.getLowerPart(), id.getUpperPart());
    }
}
