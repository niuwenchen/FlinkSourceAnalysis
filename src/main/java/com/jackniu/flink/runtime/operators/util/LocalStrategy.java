package com.jackniu.flink.runtime.operators.util;

/**
 * Created by JackNiu on 2019/7/8.
 */
public enum LocalStrategy {
    // no special local strategy is applied
    NONE(false, false),
    // the input is sorted
    SORT(true, true),
    // the input is sorted, during sorting a combiner is applied
    COMBININGSORT(true, true);

    // --------------------------------------------------------------------------------------------

    private final boolean dams;

    private boolean requiresComparator;

    private LocalStrategy(boolean dams, boolean requiresComparator) {
        this.dams = dams;
        this.requiresComparator = requiresComparator;
    }

    public boolean dams() {
        return this.dams;
    }

    public boolean requiresComparator() {
        return this.requiresComparator;
    }
}
