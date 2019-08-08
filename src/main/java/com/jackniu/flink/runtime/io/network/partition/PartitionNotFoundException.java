package com.jackniu.flink.runtime.io.network.partition;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class PartitionNotFoundException extends IOException {

    private static final long serialVersionUID = 0L;

    private final ResultPartitionID partitionId;

    public PartitionNotFoundException(ResultPartitionID partitionId) {
        this.partitionId = partitionId;
    }

    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    @Override
    public String getMessage() {
        return "Partition " + partitionId + " not found.";
    }
}
