package com.jackniu.flink.runtime.state;

import javax.annotation.Nullable;
import java.util.*;

import static com.jackniu.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface  StateSnapshotTransformer<T> {
    /**
     * Transform or filter out state values which are included or skipped in the snapshot.
     *
     * @param value non-serialized form of value
     * @return value to snapshot or null which means the entry is not included
     */
    @Nullable
    T filterOrTransform(@Nullable T value);

    /** Collection state specific transformer which says how to transform entries of the collection. */
    interface CollectionStateSnapshotTransformer<T> extends StateSnapshotTransformer<T> {
        enum TransformStrategy {
            /** Transform all entries. */
            TRANSFORM_ALL,

            /**
             * Skip first null entries.
             *
             * <p>While traversing collection entries, as optimisation, stops transforming
             * if encounters first non-null included entry and returns it plus the rest untouched.
             */
            STOP_ON_FIRST_INCLUDED
        }

        default TransformStrategy getFilterStrategy() {
            return TransformStrategy.TRANSFORM_ALL;
        }
    }

    class ListStateSnapshotTransformer<T> implements StateSnapshotTransformer<List<T>> {
        private final StateSnapshotTransformer<T> entryValueTransformer;
        private final CollectionStateSnapshotTransformer.TransformStrategy transformStrategy;

        public ListStateSnapshotTransformer(StateSnapshotTransformer<T> entryValueTransformer) {
            this.entryValueTransformer = entryValueTransformer;
            this.transformStrategy = entryValueTransformer instanceof CollectionStateSnapshotTransformer ?
                    ((CollectionStateSnapshotTransformer) entryValueTransformer).getFilterStrategy() :
                    CollectionStateSnapshotTransformer.TransformStrategy.TRANSFORM_ALL;
        }

        @Override
        @Nullable
        public List<T> filterOrTransform(@Nullable List<T> list) {
            if (list == null) {
                return null;
            }
            List<T> transformedList = new ArrayList<>();
            boolean anyChange = false;
            for (int i = 0; i < list.size(); i++) {
                T entry = list.get(i);
                T transformedEntry = entryValueTransformer.filterOrTransform(entry);
                if (transformedEntry != null) {
                    if (transformStrategy == STOP_ON_FIRST_INCLUDED) {
                        transformedList = list.subList(i, list.size());
                        anyChange = i > 0;
                        break;
                    } else {
                        transformedList.add(transformedEntry);
                    }
                }
                anyChange |= transformedEntry == null || !Objects.equals(entry, transformedEntry);
            }
            transformedList = anyChange ? transformedList : list;
            return transformedList.isEmpty() ? null : transformedList;
        }
    }
    /**
     * General implementation of map state transformer.
     *
     * <p>This transformer wraps a transformer per-entry
     * and transforms the whole map state.
     */
    class MapStateSnapshotTransformer<K, V> implements StateSnapshotTransformer<Map<K, V>> {
        private final StateSnapshotTransformer<V> entryValueTransformer;

        public MapStateSnapshotTransformer(StateSnapshotTransformer<V> entryValueTransformer) {
            this.entryValueTransformer = entryValueTransformer;
        }

        @Nullable
        @Override
        public Map<K, V> filterOrTransform(@Nullable Map<K, V> map) {
            if (map == null) {
                return null;
            }
            Map<K, V> transformedMap = new HashMap<>();
            boolean anyChange = false;
            for (Map.Entry<K, V> entry : map.entrySet()) {
                V transformedValue = entryValueTransformer.filterOrTransform(entry.getValue());
                if (transformedValue != null) {
                    transformedMap.put(entry.getKey(), transformedValue);
                }
                anyChange |= transformedValue == null || !Objects.equals(entry.getValue(), transformedValue);
            }
            return anyChange ? (transformedMap.isEmpty() ? null : transformedMap) : map;
        }
    }


    /**
     * This factory creates state transformers depending on the form of values to transform.
     *
     * <p>If there is no transforming needed, the factory methods return {@code Optional.empty()}.
     */
    interface StateSnapshotTransformFactory<T> {
        StateSnapshotTransformFactory<?> NO_TRANSFORM = createNoTransform();

        @SuppressWarnings("unchecked")
        static <T> StateSnapshotTransformFactory<T> noTransform() {
            return (StateSnapshotTransformFactory<T>) NO_TRANSFORM;
        }

        static <T> StateSnapshotTransformFactory<T> createNoTransform() {
            return new StateSnapshotTransformFactory<T>() {
                @Override
                public Optional<StateSnapshotTransformer<T>> createForDeserializedState() {
                    return Optional.empty();
                }

                @Override
                public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
                    return Optional.empty();
                }
            };
        }

        Optional<StateSnapshotTransformer<T>> createForDeserializedState();

        Optional<StateSnapshotTransformer<byte[]>> createForSerializedState();
    }
}
