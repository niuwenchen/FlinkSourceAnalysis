package com.jackniu.flink.util;

import java.util.Random;

/**
 * Created by JackNiu on 2019/7/5.
 * A statistically unique identification number.
 */
public class AbstractID implements Comparable<AbstractID>, java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private static final Random RND = new Random();

    /** The size of a long in bytes. */
    private static final int SIZE_OF_LONG = 8;

    /** The size of the ID in byte. */
    public static final int SIZE = 2 * SIZE_OF_LONG;

    // ------------------------------------------------------------------------

    /** The upper part of the actual ID. */
    protected final long upperPart;

    /** The lower part of the actual ID. */
    protected final long lowerPart;

    /** The memoized value returned by toString(). */
    private transient String toString;
    public AbstractID(byte[] bytes) {
        if (bytes == null || bytes.length != SIZE) {
            throw new IllegalArgumentException("Argument bytes must by an array of " + SIZE + " bytes");
        }

        this.lowerPart = byteArrayToLong(bytes, 0);
        this.upperPart = byteArrayToLong(bytes, SIZE_OF_LONG);
    }
    public AbstractID(long lowerPart, long upperPart) {
        this.lowerPart = lowerPart;
        this.upperPart = upperPart;
    }

    public AbstractID(AbstractID id) {
        if (id == null) {
            throw new IllegalArgumentException("Id must not be null.");
        }
        this.lowerPart = id.lowerPart;
        this.upperPart = id.upperPart;
    }

    /**
     * Constructs a new random ID from a uniform distribution.
     */
    public AbstractID() {
        this.lowerPart = RND.nextLong();
        this.upperPart = RND.nextLong();
    }

    public long getLowerPart() {
        return lowerPart;
    }

    /**
     * Gets the upper 64 bits of the ID.
     *
     * @return The upper 64 bits of the ID.
     */
    public long getUpperPart() {
        return upperPart;
    }

    /**
     * Gets the bytes underlying this ID.
     *
     * @return The bytes underlying this ID.
     */
    public byte[] getBytes() {
        byte[] bytes = new byte[SIZE];
        longToByteArray(lowerPart, bytes, 0);
        longToByteArray(upperPart, bytes, SIZE_OF_LONG);
        return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            AbstractID that = (AbstractID) obj;
            return that.lowerPart == this.lowerPart && that.upperPart == this.upperPart;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ((int)  this.lowerPart) ^
                ((int) (this.lowerPart >>> 32)) ^
                ((int)  this.upperPart) ^
                ((int) (this.upperPart >>> 32));
    }

    @Override
    public String toString() {
        if (this.toString == null) {
            final byte[] ba = new byte[SIZE];
            longToByteArray(this.lowerPart, ba, 0);
            longToByteArray(this.upperPart, ba, SIZE_OF_LONG);

            this.toString = StringUtils.byteToHexString(ba);
        }

        return this.toString;
    }

    @Override
    public int compareTo(AbstractID o) {
        int diff1 = Long.compare(this.upperPart, o.upperPart);
        int diff2 = Long.compare(this.lowerPart, o.lowerPart);
        return diff1 == 0 ? diff2 : diff1;
    }

    private static long byteArrayToLong(byte[] ba, int offset) {
        long l = 0;

        for (int i = 0; i < SIZE_OF_LONG; ++i) {
            l |= (ba[offset + SIZE_OF_LONG - 1 - i] & 0xffL) << (i << 3);
        }

        return l;
    }
    private static void longToByteArray(long l, byte[] ba, int offset) {
        for (int i = 0; i < SIZE_OF_LONG; ++i) {
            final int shift = i << 3; // i * 8
            ba[offset + SIZE_OF_LONG - 1 - i] = (byte) ((l & (0xffL << shift)) >>> shift);
        }
    }
}
