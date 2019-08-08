package com.jackniu.flink.runtime.io.network.partition;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ResultPartitionProvider {

    /**
     * Returns the requested intermediate result partition input view.
     */
    ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            int index,
            BufferAvailabilityListener availabilityListener) throws IOException;

}
