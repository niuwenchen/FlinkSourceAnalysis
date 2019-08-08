package com.jackniu.flink.runtime.state;

import com.jackniu.flink.util.StringUtils;

import java.io.ObjectStreamException;
import java.util.Arrays;

import static com.jackniu.flink.util.Preconditions.checkArgument;
import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/5.
 */
public class CheckpointStorageLocationReference implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /** The encoded location reference. null indicates the default location. */
    private final byte[] encodedReference;

    /**
     * Creates a new location reference.
     *
     * @param encodedReference The location reference, represented as bytes (non null)
     */
    public CheckpointStorageLocationReference(byte[] encodedReference) {
        checkNotNull(encodedReference);
        checkArgument(encodedReference.length > 0);


        this.encodedReference = encodedReference;
    }

    /**
     * Private constructor for singleton only.
     */
    private CheckpointStorageLocationReference() {
        this.encodedReference = null;
    }
    public byte[] getReferenceBytes() {
        // return a non null object always
        return encodedReference != null ? encodedReference : new byte[0];
    }

    /**
     * Returns true, if this object is the default reference.
     */
    public boolean isDefaultReference() {
        return encodedReference == null;
    }

    @Override
    public int hashCode() {
        return encodedReference == null ? 2059243550 : Arrays.hashCode(encodedReference);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
                obj != null && obj.getClass() == CheckpointStorageLocationReference.class &&
                        Arrays.equals(encodedReference, ((CheckpointStorageLocationReference) obj).encodedReference);
    }

    @Override
    public String toString() {
        return encodedReference == null ? "(default)"
                : StringUtils.byteToHexString(encodedReference, 0, encodedReference.length);
    }
    /**
     * readResolve() preserves the singleton property of the default value.
     */
    protected final Object readResolve() throws ObjectStreamException {
        return encodedReference == null ? DEFAULT : this;
    }

    // ------------------------------------------------------------------------
    //  Default Location Reference
    // ------------------------------------------------------------------------

    /** The singleton object for the default reference. */
    private static final CheckpointStorageLocationReference DEFAULT = new CheckpointStorageLocationReference();

    public static CheckpointStorageLocationReference getDefault() {
        return DEFAULT;
    }



}
