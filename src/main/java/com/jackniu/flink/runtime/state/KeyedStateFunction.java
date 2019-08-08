package com.jackniu.flink.runtime.state;

import com.jackniu.flink.api.common.state.State;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface KeyedStateFunction<K, S extends State>{
    /**
     * The actual method to be applied on each of the states.
     *
     * @param key the key whose state is being processed.
     * @param state the state associated with the aforementioned key.
     */
    void process(K key, S state) throws Exception;
}
