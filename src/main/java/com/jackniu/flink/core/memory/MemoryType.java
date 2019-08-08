package com.jackniu.flink.core.memory;

/**
 * Created by JackNiu on 2019/7/6.
 */
public enum  MemoryType {
    /**
     * Denotes memory that is part of the Java heap.
     */
    HEAP,

    /**
     * Denotes memory that is outside the Java heap (but still part of tha Java process).
     */
    OFF_HEAP
}
