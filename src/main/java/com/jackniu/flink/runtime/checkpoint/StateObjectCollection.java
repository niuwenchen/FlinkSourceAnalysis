package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.runtime.state.StateObject;
import com.jackniu.flink.runtime.state.StateUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;
import java.util.function.Predicate;


/**
 * Created by JackNiu on 2019/7/6.
 */
public class StateObjectCollection<T extends StateObject> implements Collection<T>, StateObject {
    private static final long serialVersionUID = 1L;

    /** The empty StateObjectCollection. */
    private static final StateObjectCollection<?> EMPTY = new StateObjectCollection<>(Collections.emptyList());

    /** Wrapped collection that contains the state objects. */
    private final Collection<T> stateObjects;

    /**
     * Creates a new StateObjectCollection that is backed by an {@link ArrayList}.
     */
    public StateObjectCollection() {
        this.stateObjects = new ArrayList<>();
    }

    /**
     * Creates a new StateObjectCollection wraps the given collection and delegates to it.
     * @param stateObjects collection of state objects to wrap.
     */
    public StateObjectCollection(Collection<T> stateObjects) {
        this.stateObjects = stateObjects != null ? stateObjects : Collections.emptyList();
    }

    @Override
    public int size() {
        return stateObjects.size();
    }

    @Override
    public boolean isEmpty() {
        return stateObjects.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return stateObjects.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return stateObjects.iterator();
    }

    @Override
    public Object[] toArray() {
        return stateObjects.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return stateObjects.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return stateObjects.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return stateObjects.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return stateObjects.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return stateObjects.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return stateObjects.removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        return stateObjects.removeIf(filter);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return stateObjects.retainAll(c);
    }

    @Override
    public void clear() {
        stateObjects.clear();
    }

    @Override
    public void discardState() throws Exception {
        StateUtil.bestEffortDiscardAllStateObjects(stateObjects);
    }

    @Override
    public long getStateSize() {
        return sumAllSizes(stateObjects);
    }

    /**
     * Returns true if this contains at least one {@link StateObject}.
     */
    public boolean hasState() {
        for (StateObject state : stateObjects) {
            if (state != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StateObjectCollection<?> that = (StateObjectCollection<?>) o;

        // simple equals can cause troubles here because of how equals works e.g. between lists and sets.
        return CollectionUtils.isEqualCollection(stateObjects, that.stateObjects);
    }

    @Override
    public int hashCode() {
        return stateObjects.hashCode();
    }

    @Override
    public String toString() {
        return "StateObjectCollection{" + stateObjects + '}';
    }

    public List<T> asList() {
        return stateObjects instanceof List ?
                (List<T>) stateObjects :
                stateObjects != null ?
                        new ArrayList<>(stateObjects) :
                        Collections.emptyList();
    }

    // ------------------------------------------------------------------------
    //  Helper methods.
    // ------------------------------------------------------------------------

    public static <T extends StateObject> StateObjectCollection<T> empty() {
        return (StateObjectCollection<T>) EMPTY;
    }

    public static <T extends StateObject> StateObjectCollection<T> singleton(T stateObject) {
        return new StateObjectCollection<>(Collections.singleton(stateObject));
    }

    private static long sumAllSizes(Collection<? extends StateObject> stateObject) {
        long size = 0L;
        for (StateObject object : stateObject) {
            size += getSizeNullSafe(object);
        }

        return size;
    }

    private static long getSizeNullSafe(StateObject stateObject) {
        return stateObject != null ? stateObject.getStateSize() : 0L;
    }
}