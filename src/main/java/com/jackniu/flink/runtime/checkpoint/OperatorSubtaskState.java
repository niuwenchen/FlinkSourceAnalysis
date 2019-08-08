package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.runtime.state.*;
import com.jackniu.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class OperatorSubtaskState  implements CompositeStateHandle {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorSubtaskState.class);

    private static final long serialVersionUID = -2394696997971923995L;


    @Nonnull
    private final StateObjectCollection<OperatorStateHandle> managedOperatorState;


    @Nonnull
    private final StateObjectCollection<OperatorStateHandle> rawOperatorState;


    @Nonnull
    private final StateObjectCollection<KeyedStateHandle> managedKeyedState;


    @Nonnull
    private final StateObjectCollection<KeyedStateHandle> rawKeyedState;

    /**
     * The state size. This is also part of the deserialized state handle.
     * We store it here in order to not deserialize the state handle when
     * gathering stats.
     */
    private final long stateSize;

    /**
     * Empty state.
     */
    public OperatorSubtaskState() {
        this(
                StateObjectCollection.empty(),
                StateObjectCollection.empty(),
                StateObjectCollection.empty(),
                StateObjectCollection.empty());
    }

    public OperatorSubtaskState(
            @Nonnull StateObjectCollection<OperatorStateHandle> managedOperatorState,
            @Nonnull StateObjectCollection<OperatorStateHandle> rawOperatorState,
            @Nonnull StateObjectCollection<KeyedStateHandle> managedKeyedState,
            @Nonnull StateObjectCollection<KeyedStateHandle> rawKeyedState) {

        this.managedOperatorState = Preconditions.checkNotNull(managedOperatorState);
        this.rawOperatorState = Preconditions.checkNotNull(rawOperatorState);
        this.managedKeyedState = Preconditions.checkNotNull(managedKeyedState);
        this.rawKeyedState = Preconditions.checkNotNull(rawKeyedState);

        long calculateStateSize = managedOperatorState.getStateSize();
        calculateStateSize += rawOperatorState.getStateSize();
        calculateStateSize += managedKeyedState.getStateSize();
        calculateStateSize += rawKeyedState.getStateSize();
        stateSize = calculateStateSize;
    }

    /**
     * For convenience because the size of the collections is typically 0 or 1. Null values are translated into empty
     * Collections.
     */
    public OperatorSubtaskState(
            @Nullable OperatorStateHandle managedOperatorState,
            @Nullable OperatorStateHandle rawOperatorState,
            @Nullable KeyedStateHandle managedKeyedState,
            @Nullable KeyedStateHandle rawKeyedState) {

        this(
                singletonOrEmptyOnNull(managedOperatorState),
                singletonOrEmptyOnNull(rawOperatorState),
                singletonOrEmptyOnNull(managedKeyedState),
                singletonOrEmptyOnNull(rawKeyedState));
    }

    private static <T extends StateObject> StateObjectCollection<T> singletonOrEmptyOnNull(T element) {
        return element != null ? StateObjectCollection.singleton(element) : StateObjectCollection.empty();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Returns a handle to the managed operator state.
     */
    @Nonnull
    public StateObjectCollection<OperatorStateHandle> getManagedOperatorState() {
        return managedOperatorState;
    }

    /**
     * Returns a handle to the raw operator state.
     */
    @Nonnull
    public StateObjectCollection<OperatorStateHandle> getRawOperatorState() {
        return rawOperatorState;
    }

    /**
     * Returns a handle to the managed keyed state.
     */
    @Nonnull
    public StateObjectCollection<KeyedStateHandle> getManagedKeyedState() {
        return managedKeyedState;
    }

    /**
     * Returns a handle to the raw keyed state.
     */
    @Nonnull
    public StateObjectCollection<KeyedStateHandle> getRawKeyedState() {
        return rawKeyedState;
    }

    @Override
    public void discardState() {
        try {
            List<StateObject> toDispose =
                    new ArrayList<>(
                            managedOperatorState.size() +
                                    rawOperatorState.size() +
                                    managedKeyedState.size() +
                                    rawKeyedState.size());
            toDispose.addAll(managedOperatorState);
            toDispose.addAll(rawOperatorState);
            toDispose.addAll(managedKeyedState);
            toDispose.addAll(rawKeyedState);
            StateUtil.bestEffortDiscardAllStateObjects(toDispose);
        } catch (Exception e) {
            LOG.warn("Error while discarding operator states.", e);
        }
    }

    @Override
    public void registerSharedStates(SharedStateRegistry sharedStateRegistry) {
        registerSharedState(sharedStateRegistry, managedKeyedState);
        registerSharedState(sharedStateRegistry, rawKeyedState);
    }

    private static void registerSharedState(
            SharedStateRegistry sharedStateRegistry,
            Iterable<KeyedStateHandle> stateHandles) {
        for (KeyedStateHandle stateHandle : stateHandles) {
            if (stateHandle != null) {
                stateHandle.registerSharedStates(sharedStateRegistry);
            }
        }
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OperatorSubtaskState that = (OperatorSubtaskState) o;

        if (getStateSize() != that.getStateSize()) {
            return false;
        }
        if (!getManagedOperatorState().equals(that.getManagedOperatorState())) {
            return false;
        }
        if (!getRawOperatorState().equals(that.getRawOperatorState())) {
            return false;
        }
        if (!getManagedKeyedState().equals(that.getManagedKeyedState())) {
            return false;
        }
        return getRawKeyedState().equals(that.getRawKeyedState());
    }

    @Override
    public int hashCode() {
        int result = getManagedOperatorState().hashCode();
        result = 31 * result + getRawOperatorState().hashCode();
        result = 31 * result + getManagedKeyedState().hashCode();
        result = 31 * result + getRawKeyedState().hashCode();
        result = 31 * result + (int) (getStateSize() ^ (getStateSize() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "SubtaskState{" +
                "operatorStateFromBackend=" + managedOperatorState +
                ", operatorStateFromStream=" + rawOperatorState +
                ", keyedStateFromBackend=" + managedKeyedState +
                ", keyedStateFromStream=" + rawKeyedState +
                ", stateSize=" + stateSize +
                '}';
    }

    public boolean hasState() {
        return managedOperatorState.hasState()
                || rawOperatorState.hasState()
                || managedKeyedState.hasState()
                || rawKeyedState.hasState();
    }
}

