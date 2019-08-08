package com.jackniu.flink.api.common.accumulators;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class Histogram implements Accumulator<Integer, TreeMap<Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    private TreeMap<Integer, Integer> treeMap = new TreeMap<Integer, Integer>();

    @Override
    public void add(Integer value) {
        Integer current = treeMap.get(value);
        Integer newValue = (current != null ? current : 0) + 1;
        this.treeMap.put(value, newValue);
    }

    @Override
    public TreeMap<Integer, Integer> getLocalValue() {
        return this.treeMap;
    }

    @Override
    public void merge(Accumulator<Integer, TreeMap<Integer, Integer>> other) {
        // Merge the values into this map
        for (Map.Entry<Integer, Integer> entryFromOther : other.getLocalValue().entrySet()) {
            Integer ownValue = this.treeMap.get(entryFromOther.getKey());
            if (ownValue == null) {
                this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue());
            } else {
                this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue() + ownValue);
            }
        }
    }

    @Override
    public void resetLocal() {
        this.treeMap.clear();
    }

    @Override
    public String toString() {
        return this.treeMap.toString();
    }

    @Override
    public Accumulator<Integer, TreeMap<Integer, Integer>> clone() {
        Histogram result = new Histogram();
        result.treeMap = new TreeMap<Integer, Integer>(treeMap);
        return result;
    }
}
