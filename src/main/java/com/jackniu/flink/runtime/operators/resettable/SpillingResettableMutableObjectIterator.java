package com.jackniu.flink.runtime.operators.resettable;

import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.MemorySegment;
import com.jackniu.flink.runtime.io.disk.SpillingBuffer;
import com.jackniu.flink.runtime.io.disk.iomanager.IOManager;
import com.jackniu.flink.runtime.jobgraph.tasks.AbstractInvokable;
import com.jackniu.flink.runtime.memory.ListMemorySegmentSource;
import com.jackniu.flink.runtime.memory.MemoryAllocationException;
import com.jackniu.flink.runtime.memory.MemoryManager;
import com.jackniu.flink.util.MutableObjectIterator;
import com.jackniu.flink.util.ResettableMutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class SpillingResettableMutableObjectIterator<T> implements ResettableMutableObjectIterator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SpillingResettableMutableObjectIterator.class);

    // ------------------------------------------------------------------------

    protected DataInputView inView;

    protected final TypeSerializer<T> serializer;

    private int elementCount;

    private int currentElementNum;

    protected final SpillingBuffer buffer;

    protected final MutableObjectIterator<T> input;

    protected final MemoryManager memoryManager;

    private final List<MemorySegment> memorySegments;

    private final boolean releaseMemoryOnClose;

    // ------------------------------------------------------------------------

    public SpillingResettableMutableObjectIterator(MutableObjectIterator<T> input, TypeSerializer<T> serializer,
                                                   MemoryManager memoryManager, IOManager ioManager,
                                                   int numPages, AbstractInvokable parentTask)
            throws MemoryAllocationException
    {
        this(input, serializer, memoryManager, ioManager, memoryManager.allocatePages(parentTask, numPages), true);
    }

    public SpillingResettableMutableObjectIterator(MutableObjectIterator<T> input, TypeSerializer<T> serializer,
                                                   MemoryManager memoryManager, IOManager ioManager,
                                                   List<MemorySegment> memory)
    {
        this(input, serializer, memoryManager, ioManager, memory, false);
    }

    private SpillingResettableMutableObjectIterator(MutableObjectIterator<T> input, TypeSerializer<T> serializer,
                                                    MemoryManager memoryManager, IOManager ioManager,
                                                    List<MemorySegment> memory, boolean releaseMemOnClose)
    {
        this.memoryManager = memoryManager;
        this.input = input;
        this.serializer = serializer;
        this.memorySegments = memory;
        this.releaseMemoryOnClose = releaseMemOnClose;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating spilling resettable iterator with " + memory.size() + " pages of memory.");
        }

        this.buffer = new SpillingBuffer(ioManager, new ListMemorySegmentSource(memory), memoryManager.getPageSize());
    }

    public void open() {}


    @Override
    public void reset() throws IOException {
        this.inView = this.buffer.flip();
        this.currentElementNum = 0;
    }

    public List<MemorySegment> close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Spilling Resettable Iterator closing. Stored " + this.elementCount + " records.");
        }

        this.inView = null;

        final List<MemorySegment> memory = this.buffer.close();
        memory.addAll(this.memorySegments);
        this.memorySegments.clear();

        if (this.releaseMemoryOnClose) {
            this.memoryManager.release(memory);
            return Collections.emptyList();
        } else {
            return memory;
        }
    }

    @Override
    public T next(T reuse) throws IOException {
        if (this.inView != null) {
            // reading, any subsequent pass
            if (this.currentElementNum < this.elementCount) {
                try {
                    reuse = this.serializer.deserialize(reuse, this.inView);
                } catch (IOException e) {
                    throw new RuntimeException("SpillingIterator: Error reading element from buffer.", e);
                }
                this.currentElementNum++;
                return reuse;
            } else {
                return null;
            }
        } else {
            // writing pass (first)
            if ((reuse = this.input.next(reuse)) != null) {
                try {
                    this.serializer.serialize(reuse, this.buffer);
                } catch (IOException e) {
                    throw new RuntimeException("SpillingIterator: Error writing element to buffer.", e);
                }
                this.elementCount++;
                return reuse;
            } else {
                return null;
            }
        }
    }

    @Override
    public T next() throws IOException {
        T result = null;
        if (this.inView != null) {
            // reading, any subsequent pass
            if (this.currentElementNum < this.elementCount) {
                try {
                    result = this.serializer.deserialize(this.inView);
                } catch (IOException e) {
                    throw new RuntimeException("SpillingIterator: Error reading element from buffer.", e);
                }
                this.currentElementNum++;
                return result;
            } else {
                return null;
            }
        } else {
            // writing pass (first)
            if ((result = this.input.next()) != null) {
                try {
                    this.serializer.serialize(result, this.buffer);
                } catch (IOException e) {
                    throw new RuntimeException("SpillingIterator: Error writing element to buffer.", e);
                }
                this.elementCount++;
                return result;
            } else {
                return null;
            }
        }
    }


    public void consumeAndCacheRemainingData() throws IOException {
        // check that we are in the first pass and that more input data is left
        if (this.inView == null) {
            T holder = this.serializer.createInstance();

            while ((holder = this.input.next(holder)) != null) {
                try {
                    this.serializer.serialize(holder, this.buffer);
                } catch (IOException e) {
                    throw new RuntimeException("SpillingIterator: Error writing element to buffer.", e);
                }
                this.elementCount++;
            }
        }
    }
}
