package com.jackniu.flink.runtime.checkpoint;

import java.util.Arrays;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class MasterState implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final byte[] bytes;
    private final int version;

    public MasterState(String name, byte[] bytes, int version) {
        this.name = checkNotNull(name);
        this.bytes = checkNotNull(bytes);
        this.version = version;
    }

    // ------------------------------------------------------------------------

    public String name() {
        return name;
    }

    public byte[] bytes() {
        return bytes;
    }

    public int version() {
        return version;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "name: " + name + " ; version: " + version + " ; bytes: " + Arrays.toString(bytes);
    }
}
