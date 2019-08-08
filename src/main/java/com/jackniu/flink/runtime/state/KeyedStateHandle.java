package com.jackniu.flink.runtime.state;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface KeyedStateHandle extends CompositeStateHandle {

    /**
     * Returns the range of the key groups contained in the state.
     */
    KeyGroupRange getKeyGroupRange();

    /**
     * Returns a state over a range that is the intersection between this
     * handle's key-group range and the provided key-group range.
     *
     * @param keyGroupRange The key group range to intersect with
     */
    KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange);
}

