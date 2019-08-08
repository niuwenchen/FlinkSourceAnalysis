package com.jackniu.flink.runtime.clusterframework.types;

import com.jackniu.flink.util.AbstractID;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class AllocationID extends AbstractID {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new random AllocationID.
     */
    public AllocationID() {
        super();
    }

    /**
     * Constructs a new AllocationID with the given parts.
     *
     * @param lowerPart the lower bytes of the ID
     * @param upperPart the higher bytes of the ID
     */
    public AllocationID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    @Override
    public String toString() {
        return "AllocationID{" + super.toString() + '}';
    }
}

