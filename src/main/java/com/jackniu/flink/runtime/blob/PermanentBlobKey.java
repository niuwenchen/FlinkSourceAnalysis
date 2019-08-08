package com.jackniu.flink.runtime.blob;

import com.jackniu.flink.annotations.VisibleForTesting;

/**
 * Created by JackNiu on 2019/7/7.
 */
public final class PermanentBlobKey extends BlobKey {
    /**
     * Constructs a new BLOB key.
     */
    @VisibleForTesting
    public PermanentBlobKey() {
        super(BlobType.PERMANENT_BLOB);
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key
     *        the actual key data
     */
    PermanentBlobKey(byte[] key) {
        super(BlobType.PERMANENT_BLOB, key);
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key
     *        the actual key data
     * @param random
     *        the random component of the key
     */
    PermanentBlobKey(byte[] key, byte[] random) {
        super(BlobType.PERMANENT_BLOB, key, random);
    }

}
