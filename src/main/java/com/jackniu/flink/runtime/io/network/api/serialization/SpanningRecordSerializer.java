package com.jackniu.flink.runtime.io.network.api.serialization;

import com.jackniu.flink.core.io.IOReadableWritable;
import com.jackniu.flink.core.memory.DataOutputSerializer;
import com.jackniu.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class SpanningRecordSerializer<T extends IOReadableWritable> implements RecordSerializer<T> {

    /** Flag to enable/disable checks, if buffer not set/full or pending serialization. */
    private static final boolean CHECKED = false;

    /** Intermediate data serialization. */
    private final DataOutputSerializer serializationBuffer;

    /** Intermediate buffer for data serialization (wrapped from {@link #serializationBuffer}). */
    private ByteBuffer dataBuffer;

    /** Intermediate buffer for length serialization. */
    private final ByteBuffer lengthBuffer;

    public SpanningRecordSerializer() {
        serializationBuffer = new DataOutputSerializer(128);

        lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.order(ByteOrder.BIG_ENDIAN);

        // ensure initial state with hasRemaining false (for correct continueWritingWithNextBufferBuilder logic)
        dataBuffer = serializationBuffer.wrapAsByteBuffer();
        lengthBuffer.position(4);
    }

    /**
     * Serializes the complete record to an intermediate data serialization buffer.
     *
     * @param record the record to serialize
     */
    @Override
    public void serializeRecord(T record) throws IOException {
        if (CHECKED) {
            if (dataBuffer.hasRemaining()) {
                throw new IllegalStateException("Pending serialization of previous record.");
            }
        }

        serializationBuffer.clear();
        lengthBuffer.clear();

        // write data and length
        record.write(serializationBuffer);

        int len = serializationBuffer.length();
        lengthBuffer.putInt(0, len);

        dataBuffer = serializationBuffer.wrapAsByteBuffer();
    }

    /**
     * Copies an intermediate data serialization buffer into the target BufferBuilder.
     *
     * @param targetBuffer the target BufferBuilder to copy to
     * @return how much information was written to the target buffer and
     *         whether this buffer is full
     */
    @Override
    public SerializationResult copyToBufferBuilder(BufferBuilder targetBuffer) {
        targetBuffer.append(lengthBuffer);
        targetBuffer.append(dataBuffer);
        targetBuffer.commit();

        return getSerializationResult(targetBuffer);
    }

    private SerializationResult getSerializationResult(BufferBuilder targetBuffer) {
        if (dataBuffer.hasRemaining() || lengthBuffer.hasRemaining()) {
            return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
        }
        return !targetBuffer.isFull()
                ? SerializationResult.FULL_RECORD
                : SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
    }

    @Override
    public void reset() {
        dataBuffer.position(0);
        lengthBuffer.position(0);
    }

    @Override
    public void prune() {
        serializationBuffer.pruneBuffer();
        dataBuffer = serializationBuffer.wrapAsByteBuffer();
    }

    @Override
    public boolean hasSerializedData() {
        return lengthBuffer.hasRemaining() || dataBuffer.hasRemaining();
    }
}

