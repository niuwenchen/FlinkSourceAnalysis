package com.jackniu.flink.runtime.state.internal;

/**
 * Created by JackNiu on 2019/7/6.
 */

import com.jackniu.flink.api.common.state.State;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
/**
        * The {@code InternalKvState} is the root of the internal state type hierarchy, similar to the
        * {@link State} being the root of the public API state hierarchy.
        *
        * <p>The internal state classes give access to the namespace getters and setters and access to
        * additional functionality, like raw value access or state merging.
        *
        * <p>The public API state hierarchy is intended to be programmed against by Flink applications.
        * The internal state hierarchy holds all the auxiliary methods that are used by the runtime and not
        * intended to be used by user applications. These internal methods are considered of limited use to users and
        * only confusing, and are usually not regarded as stable across releases.
        *
        * <p>Each specific type in the internal state hierarchy extends the type from the public
 * state hierarchy:
         *
         * <pre>
 *             State
         *               |
         *               +-------------------InternalKvState
         *               |                         |
         *          MergingState                   |
         *               |                         |
         *               +-----------------InternalMergingState
         *               |                         |
         *      +--------+------+                  |
         *      |               |                  |
         * ReducingState    ListState        +-----+-----------------+
         *      |               |            |                       |
         *      +-----------+   +-----------   -----------------InternalListState
         *                  |                |
         *                  +---------InternalReducingState
         * </pre>
        *
        * @param <K> The type of key the state is associated to
        * @param <N> The type of the namespace
        * @param <V> The type of values kept internally in state
        */
public interface InternalKvState<K, N, V> extends State {

    /**
     * Returns the {@link TypeSerializer} for the type of key this state is associated to.
     */
    TypeSerializer<K> getKeySerializer();

    /**
     * Returns the {@link TypeSerializer} for the type of namespace this state is associated to.
     */
    TypeSerializer<N> getNamespaceSerializer();

    /**
     * Returns the {@link TypeSerializer} for the type of value this state holds.
     */
    TypeSerializer<V> getValueSerializer();

    /**
     * Sets the current namespace, which will be used when using the state access methods.
     *
     * @param namespace The namespace.
     */
    void setCurrentNamespace(N namespace);

    /**
     * Returns the serialized value for the given key and namespace.
     *
     * <p>If no value is associated with key and namespace, <code>null</code>
     * is returned.
     *
     * <p><b>TO IMPLEMENTERS:</b> This method is called by multiple threads. Anything
     * stateful (e.g. serializers) should be either duplicated or protected from undesired
     * consequences of concurrent invocations.
     *
     * @param serializedKeyAndNamespace Serialized key and namespace
     * @param safeKeySerializer A key serializer which is safe to be used even in multi-threaded context
     * @param safeNamespaceSerializer A namespace serializer which is safe to be used even in multi-threaded context
     * @param safeValueSerializer A value serializer which is safe to be used even in multi-threaded context
     * @return Serialized value or <code>null</code> if no value is associated with the key and namespace.
     *
     * @throws Exception Exceptions during serialization are forwarded
     */
    byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer) throws Exception;
}
