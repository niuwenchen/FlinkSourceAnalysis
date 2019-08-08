package com.jackniu.flink.api.common.accumulators;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class LongCounter implements SimpleAccumulator<Long> {

    private static final long serialVersionUID = 1L;

    private long localValue;

    public LongCounter() {}

    public LongCounter(long value) {
        this.localValue = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator
    // ------------------------------------------------------------------------

    /**
     * Consider using {@link #add(long)} instead for primitive long values
     */
    @Override
    public void add(Long value) {
        this.localValue += value;
    }

    @Override
    public Long getLocalValue() {
        return this.localValue;
    }

    @Override
    public void merge(Accumulator<Long, Long> other) {
        this.localValue += other.getLocalValue();
    }

    @Override
    public void resetLocal() {
        this.localValue = 0;
    }

    @Override
    public LongCounter clone() {
        LongCounter result = new LongCounter();
        result.localValue = localValue;
        return result;
    }

    // ------------------------------------------------------------------------
    //  Primitive Specializations
    // ------------------------------------------------------------------------

    public void add(long value){
        this.localValue += value;
    }

    public long getLocalValuePrimitive() {
        return this.localValue;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "LongCounter " + this.localValue;
    }
}

