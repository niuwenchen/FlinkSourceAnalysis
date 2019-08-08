package com.jackniu.flink.runtime.state;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface KeyGroupedInternalPriorityQueue<T> extends InternalPriorityQueue<T> {
    @Nonnull
    Set<T> getSubsetForKeyGroup(int keyGroupId);
}
