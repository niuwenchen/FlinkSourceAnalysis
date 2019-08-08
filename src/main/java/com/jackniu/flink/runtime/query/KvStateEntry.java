package com.jackniu.flink.runtime.query;

import com.jackniu.flink.annotations.VisibleForTesting;
import com.jackniu.flink.runtime.state.internal.InternalKvState;
import com.jackniu.flink.util.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class KvStateEntry<K,N,V> {
    private final InternalKvState<K, N, V> state;
    private final KvStateInfo<K, N, V> stateInfo;

    private final boolean areSerializersStateless;

    private final ConcurrentMap<Thread, KvStateInfo<K, N, V>> serializerCache;

    public KvStateEntry(final InternalKvState<K, N, V> state) {
        this.state = Preconditions.checkNotNull(state);
        this.stateInfo = new KvStateInfo<>(
                state.getKeySerializer(),
                state.getNamespaceSerializer(),
                state.getValueSerializer()
        );
        this.serializerCache = new ConcurrentHashMap<>();
        this.areSerializersStateless = stateInfo.duplicate() == stateInfo;
    }

    public InternalKvState<K, N, V> getState() {
        return state;
    }

    public KvStateInfo<K, N, V> getInfoForCurrentThread() {
        return areSerializersStateless
                ? stateInfo
                : serializerCache.computeIfAbsent(Thread.currentThread(), t -> stateInfo.duplicate());
    }

    public void clear() {
        serializerCache.clear();
    }

    @VisibleForTesting
    public int getCacheSize() {
        return serializerCache.size();
    }
}
