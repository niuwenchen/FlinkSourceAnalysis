package com.jackniu.flink.runtime.state;

import java.io.Closeable;
import java.util.Collection;

/**
 * Created by JackNiu on 2019/7/5.
 */
public abstract class AbstractKeyedStateBackend<K> implements
        KeyedStateBackend<K>,
        Snapshotable<SnapshotResult<KeyedStateHandle>, Collection<KeyedStateHandle>>,
        Closeable,
        CheckpointListener {
}
