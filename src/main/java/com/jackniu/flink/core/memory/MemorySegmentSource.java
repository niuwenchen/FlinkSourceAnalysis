package com.jackniu.flink.core.memory;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface MemorySegmentSource {
    MemorySegment nextSegment();
}
