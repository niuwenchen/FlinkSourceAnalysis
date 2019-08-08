package com.jackniu.flink.api.common.state;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface AppendingState<IN, OUT> extends State {

    /**
     * Returns the current value for the state. When the state is not
     * partitioned the returned value is the same for all inputs in a given
     * operator instance. If state partitioning is applied, the value returned
     * depends on the current operator input, as the operator maintains an
     * independent state for each partition.
     *
     * <p><b>NOTE TO IMPLEMENTERS:</b> if the state is empty, then this method
     * should return {@code null}.
     *
     * @return The operator state value corresponding to the current input or {@code null}
     * if the state is empty.
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    OUT get() throws Exception;

    /**
     * Updates the operator state accessible by {@link #get()} by adding the given value
     * to the list of values. The next time {@link #get()} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>If null is passed in, the state value will remain unchanged.
     *
     * @param value The new value for the state.
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    void add(IN value) throws Exception;

}
