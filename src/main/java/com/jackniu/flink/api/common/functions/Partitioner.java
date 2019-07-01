package com.jackniu.flink.api.common.functions;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface Partitioner<K> extends Function,Serializable {
    int partition(K key,int numPartitions);
}
