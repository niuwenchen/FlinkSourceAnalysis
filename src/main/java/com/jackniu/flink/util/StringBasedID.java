package com.jackniu.flink.util;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class StringBasedID implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Uses a String as internal representation.
     */
    private final String keyString;

    /**
     * Protected constructor to enforce that subclassing.
     */
    protected StringBasedID(String keyString) {
        this.keyString = Preconditions.checkNotNull(keyString);
    }

    public String getKeyString() {
        return keyString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StringBasedID that = (StringBasedID) o;
        return keyString.equals(that.keyString);
    }

    @Override
    public int hashCode() {
        return keyString.hashCode();
    }

    @Override
    public String toString() {
        return keyString;
    }
}

