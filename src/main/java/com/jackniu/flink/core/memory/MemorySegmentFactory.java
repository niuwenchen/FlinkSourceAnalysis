package com.jackniu.flink.core.memory;

import java.nio.ByteBuffer;

/**
 * Created by JackNiu on 2019/7/6.
 */
public final class MemorySegmentFactory {
    /**
     * Creates a new memory segment that targets the given heap memory region.
     *
     * <p>This method should be used to turn short lived byte arrays into memory segments.
     *
     * @param buffer The heap memory region.
     * @return A new memory segment that targets the given heap memory region.
     */
    public static MemorySegment wrap(byte[] buffer) {
        return new HybridMemorySegment(buffer);
    }

    /**
     * Allocates some unpooled memory and creates a new memory segment that represents
     * that memory.
     *
     * <p>This method is similar to {@link #allocateUnpooledSegment(int, Object)}, but the
     * memory segment will have null as the owner.
     *
     * @param size The size of the memory segment to allocate.
     * @return A new memory segment, backed by unpooled heap memory.
     */
    public static MemorySegment allocateUnpooledSegment(int size) {
        return allocateUnpooledSegment(size, null);
    }

    /**
     * Allocates some unpooled memory and creates a new memory segment that represents
     * that memory.
     *
     * <p>This method is similar to {@link #allocateUnpooledSegment(int)}, but additionally sets
     * the owner of the memory segment.
     *
     * @param size The size of the memory segment to allocate.
     * @param owner The owner to associate with the memory segment.
     * @return A new memory segment, backed by unpooled heap memory.
     */
    public static MemorySegment allocateUnpooledSegment(int size, Object owner) {
        return new HybridMemorySegment(new byte[size], owner);
    }

    /**
     * Allocates some unpooled off-heap memory and creates a new memory segment that
     * represents that memory.
     *
     * @param size The size of the off-heap memory segment to allocate.
     * @param owner The owner to associate with the off-heap memory segment.
     * @return A new memory segment, backed by unpooled off-heap memory.
     */
    public static MemorySegment allocateUnpooledOffHeapMemory(int size, Object owner) {
        ByteBuffer memory = ByteBuffer.allocateDirect(size);
        return wrapPooledOffHeapMemory(memory, owner);
    }

    /**
     * Creates a memory segment that wraps the given byte array.
     *
     * <p>This method is intended to be used for components which pool memory and create
     * memory segments around long-lived memory regions.
     *
     * @param memory The heap memory to be represented by the memory segment.
     * @param owner The owner to associate with the memory segment.
     * @return A new memory segment representing the given heap memory.
     */
    public static MemorySegment wrapPooledHeapMemory(byte[] memory, Object owner) {
        return new HybridMemorySegment(memory, owner);
    }

    /**
     * Creates a memory segment that wraps the off-heap memory backing the given ByteBuffer.
     * Note that the ByteBuffer needs to be a <i>direct ByteBuffer</i>.
     *
     * <p>This method is intended to be used for components which pool memory and create
     * memory segments around long-lived memory regions.
     *
     * @param memory The byte buffer with the off-heap memory to be represented by the memory segment.
     * @param owner The owner to associate with the memory segment.
     * @return A new memory segment representing the given off-heap memory.
     */
    public static MemorySegment wrapPooledOffHeapMemory(ByteBuffer memory, Object owner) {
        return new HybridMemorySegment(memory, owner);
    }

}
