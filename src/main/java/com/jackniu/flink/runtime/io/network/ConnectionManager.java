package com.jackniu.flink.runtime.io.network;

import com.jackniu.flink.runtime.io.network.nerry.PartitionRequestClient;
import com.jackniu.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ConnectionManager {void start(ResultPartitionProvider partitionProvider,
                                               TaskEventDispatcher taskEventDispatcher) throws IOException;

    /**
     * Creates a {@link PartitionRequestClient} instance for the given {@link ConnectionID}.
     */
    PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException;

    /**
     * Closes opened ChannelConnections in case of a resource release.
     */
    void closeOpenChannelConnections(ConnectionID connectionId);

    int getNumberOfActiveConnections();

    int getDataPort();

    void shutdown() throws IOException;

}
