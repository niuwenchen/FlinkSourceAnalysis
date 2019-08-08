package com.jackniu.flink.runtime.state;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface PriorityComparator<T> {
    PriorityComparator<? extends PriorityComparable<Object>> FOR_PRIORITY_COMPARABLE_OBJECTS = PriorityComparable::comparePriorityTo;

    /**
     * Compares two objects for priority. Returns a negative integer, zero, or a positive integer as the first
     * argument has lower, equal to, or higher priority than the second.
     * @param left left operand in the comparison by priority.
     * @param right left operand in the comparison by priority.
     * @return a negative integer, zero, or a positive integer as the first argument has lower, equal to, or higher
     * priority than the second.
     */
    int comparePriority(T left, T right);

    @SuppressWarnings("unchecked")
    static <T extends PriorityComparable<?>> PriorityComparator<T> forPriorityComparableObjects() {
        return (PriorityComparator<T>) FOR_PRIORITY_COMPARABLE_OBJECTS;
    }
}
