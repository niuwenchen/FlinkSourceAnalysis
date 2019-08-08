package com.jackniu.flink.runtime.state;

import com.jackniu.flink.api.common.state.State;
import com.jackniu.flink.api.common.state.StateDescriptor;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface KeyedStateFactory {
    /**
     * Creates and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     *
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    default <N, SV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc) throws Exception {
        return createInternalState(namespaceSerializer, stateDesc, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
    }

    @Nonnull
    <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception;
}
