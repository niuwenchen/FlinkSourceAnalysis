package com.jackniu.flink.api.common.accumulators;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class IntCounter implements SimpleAccumulator<Integer> {

    private static final long serialVersionUID = 1L;

    private int localValue = 0;

    public IntCounter() {}

    public IntCounter(int value) {
        this.localValue = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator
    // ------------------------------------------------------------------------

    /**
     * Consider using {@link #add(int)} instead for primitive int values
     */
    @Override
    public void add(Integer value) {
        localValue += value;
    }

    @Override
    public Integer getLocalValue() {
        return localValue;
    }

    @Override
    public void merge(Accumulator<Integer, Integer> other) {
        this.localValue += other.getLocalValue();
    }

    @Override
    public void resetLocal() {
        this.localValue = 0;
    }

    @Override
    public IntCounter clone() {
        IntCounter result = new IntCounter();
        result.localValue = localValue;
        return result;
    }

    // ------------------------------------------------------------------------
    //  Primitive Specializations
    // ------------------------------------------------------------------------

    public void add(int value){
        localValue += value;
    }

    public int getLocalValuePrimitive() {
        return this.localValue;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "IntCounter " + this.localValue;
    }
}

