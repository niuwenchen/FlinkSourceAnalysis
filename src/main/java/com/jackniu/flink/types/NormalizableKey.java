package com.jackniu.flink.types;

import com.jackniu.flink.core.memory.MemorySegment;

/**
 * Created by JackNiu on 2019/6/19.
 */
public interface NormalizableKey<T> extends Comparable<T>, Key<T> {
    int getMaxNormalizedKeyLen();
    void copyNormalizedKey(MemorySegment memory, int offset, int len);
}
