package com.jackniu.flink.runtime.jobgraph;

import com.jackniu.flink.util.AbstractID;
import com.jackniu.flink.util.StringUtils;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class JobVertexID extends AbstractID {
    private static final long serialVersionUID = 1L;

    public JobVertexID() {
        super();
    }
    public JobVertexID(byte[] bytes) {
        super(bytes);
    }

    public JobVertexID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    public static JobVertexID fromHexString(String hexString) {
        return new JobVertexID(StringUtils.hexStringToByte(hexString));
    }
}
