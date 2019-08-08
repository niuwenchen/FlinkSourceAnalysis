package com.jackniu.flink.runtime.state;

import com.jackniu.flink.api.common.state.State;
import com.jackniu.flink.api.common.state.StateDescriptor;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.runtime.state.heap.InternalKeyContext;
import com.jackniu.flink.util.Disposable;

import java.util.stream.Stream;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface KeyedStateBackend<K>
        extends InternalKeyContext<K>, KeyedStateFactory, PriorityQueueSetFactory, Disposable {
    void setCurrentKey(K newKey);
    <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function) throws Exception;

    <N> Stream<K> getKeys(String state, N namespace);

    <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor) throws Exception;

    <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor) throws Exception;

    @Override
    void dispose();
}
