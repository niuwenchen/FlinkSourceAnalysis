package com.jackniu.flink.runtime.executiongraph;

/**
 * Created by JackNiu on 2019/7/6.
 */

import com.jackniu.flink.util.AbstractID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Unique identifier for the attempt to execute a tasks. Multiple attempts happen
 * in cases of failures and recovery.
 */

public class ExecutionAttemptID extends AbstractID {
    private static final long serialVersionUID = -1169683445778281344L;

    public ExecutionAttemptID() {
    }

    public ExecutionAttemptID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    public void writeTo(ByteBuf buf) {
        buf.writeLong(this.lowerPart);
        buf.writeLong(this.upperPart);
    }

    public static ExecutionAttemptID fromByteBuf(ByteBuf buf) {
        long lower = buf.readLong();
        long upper = buf.readLong();
        return new ExecutionAttemptID(lower, upper);
    }
}
