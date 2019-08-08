package com.jackniu.flink.runtime.io.network.api.reader;

import com.jackniu.flink.core.io.IOReadableWritable;
import com.jackniu.flink.runtime.io.network.api.serialization.RecordDeserializer;
import com.jackniu.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import com.jackniu.flink.runtime.io.network.buffer.Buffer;
import com.jackniu.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import com.jackniu.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
abstract class AbstractRecordReader<T extends IOReadableWritable> extends AbstractReader implements ReaderBase {

    private final RecordDeserializer<T>[] recordDeserializers;

    private RecordDeserializer<T> currentRecordDeserializer;

    private boolean isFinished;

    /**
     * Creates a new AbstractRecordReader that de-serializes records from the given input gate and
     * can spill partial records to disk, if they grow large.
     *
     * @param inputGate The input gate to read from.
     * @param tmpDirectories The temp directories. USed for spilling if the reader concurrently
     *                       reconstructs multiple large records.
     */
    @SuppressWarnings("unchecked")
    protected AbstractRecordReader(InputGate inputGate, String[] tmpDirectories) {
        super(inputGate);

        // Initialize one deserializer per input channel
        this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
        for (int i = 0; i < recordDeserializers.length; i++) {
            recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<T>(tmpDirectories);
        }
    }

    protected boolean getNextRecord(T target) throws IOException, InterruptedException {
        if (isFinished) {
            return false;
        }

        while (true) {
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result = currentRecordDeserializer.getNextRecord(target);

                if (result.isBufferConsumed()) {
                    final Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();

                    currentBuffer.recycleBuffer();
                    currentRecordDeserializer = null;
                }

                if (result.isFullRecord()) {
                    return true;
                }
            }

            final BufferOrEvent bufferOrEvent = inputGate.getNextBufferOrEvent().orElseThrow(IllegalStateException::new);

            if (bufferOrEvent.isBuffer()) {
                currentRecordDeserializer = recordDeserializers[bufferOrEvent.getChannelIndex()];
                currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
            }
            else {
                // sanity check for leftover data in deserializers. events should only come between
                // records, not in the middle of a fragment
                if (recordDeserializers[bufferOrEvent.getChannelIndex()].hasUnfinishedData()) {
                    throw new IOException(
                            "Received an event in channel " + bufferOrEvent.getChannelIndex() + " while still having "
                                    + "data from a record. This indicates broken serialization logic. "
                                    + "If you are using custom serialization code (Writable or Value types), check their "
                                    + "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
                }

                if (handleEvent(bufferOrEvent.getEvent())) {
                    if (inputGate.isFinished()) {
                        isFinished = true;
                        return false;
                    }
                    else if (hasReachedEndOfSuperstep()) {
                        return false;
                    }
                    // else: More data is coming...
                }
            }
        }
    }

    public void clearBuffers() {
        for (RecordDeserializer<?> deserializer : recordDeserializers) {
            Buffer buffer = deserializer.getCurrentBuffer();
            if (buffer != null && !buffer.isRecycled()) {
                buffer.recycleBuffer();
            }
            deserializer.clear();
        }
    }
}
