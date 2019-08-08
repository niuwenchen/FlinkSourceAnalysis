package com.jackniu.flink.runtime.state;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface Keyed<K> {
    K getKey();
}
