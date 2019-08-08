package com.jackniu.flink.runtime.io.network.api.writer;

import com.jackniu.flink.runtime.io.network.buffer.BufferConsumer;
import com.jackniu.flink.runtime.io.network.buffer.BufferProvider;
import com.jackniu.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ResultPartitionWriter {
    BufferProvider getBufferProvider();

    ResultPartitionID getPartitionId();

    int getNumberOfSubpartitions();

    int getNumTargetKeyGroups();

    /**
     * Adds the bufferConsumer to the subpartition with the given index.
     *
     * <p>For PIPELINED {@link }s,
     * this will trigger the deployment of consuming tasks after the first buffer has been added.
     *
     * <p>This method takes the ownership of the passed {@code bufferConsumer} and thus is responsible for releasing
     * it's resources.
     *
     * <p>To avoid problems with data re-ordering, before adding new {@link BufferConsumer} the previously added one
     * the given {@code subpartitionIndex} must be marked as {@link BufferConsumer#isFinished()}.
     */
    void addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException;

    /**
     * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in all subpartitions.
     */
    void flushAll();

    /**
     * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in one specified subpartition.
     */
    void flush(int subpartitionIndex);
}
