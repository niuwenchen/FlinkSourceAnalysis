package com.jackniu.flink.runtime.state;

import javax.annotation.Nonnull;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface PriorityComparable<T> {
    int comparePriorityTo(@Nonnull T other);
}
