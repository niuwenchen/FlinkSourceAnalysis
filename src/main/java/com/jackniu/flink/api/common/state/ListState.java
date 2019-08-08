package com.jackniu.flink.api.common.state;

import java.util.List;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface ListState<T> extends MergingState<T, Iterable<T>> {

    /**
     * Updates the operator state accessible by {@link #get()} by updating existing values to
     * to the given list of values. The next time {@link #get()} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>If null or an empty list is passed in, the state value will be null.
     *
     * @param values The new values for the state.
     *
     * @throws Exception The method may forward exception thrown internally (by I/O or functions).
     */
    void update(List<T> values) throws Exception;

    /**
     * Updates the operator state accessible by {@link #get()} by adding the given values
     * to existing list of values. The next time {@link #get()} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>If null or an empty list is passed in, the state value remains unchanged.
     *
     * @param values The new values to be added to the state.
     *
     * @throws Exception The method may forward exception thrown internally (by I/O or functions).
     */
    void addAll(List<T> values) throws Exception;
}
