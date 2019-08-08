package com.jackniu.flink.runtime.io.network.api.reader;

import com.jackniu.flink.core.io.IOReadableWritable;
import com.jackniu.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class MutableRecordReader<T extends IOReadableWritable> extends AbstractRecordReader<T> implements MutableReader<T> {

    /**
     * Creates a new MutableRecordReader that de-serializes records from the given input gate and
     * can spill partial records to disk, if they grow large.
     *
     * @param inputGate The input gate to read from.
     * @param tmpDirectories The temp directories. USed for spilling if the reader concurrently
     *                       reconstructs multiple large records.
     */
    public MutableRecordReader(InputGate inputGate, String[] tmpDirectories) {
        super(inputGate, tmpDirectories);
    }

    @Override
    public boolean next(final T target) throws IOException, InterruptedException {
        return getNextRecord(target);
    }

    @Override
    public void clearBuffers() {
        super.clearBuffers();
    }
}
