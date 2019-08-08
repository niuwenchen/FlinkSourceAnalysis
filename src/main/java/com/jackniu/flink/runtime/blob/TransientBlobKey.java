package com.jackniu.flink.runtime.blob;

import com.jackniu.flink.annotations.VisibleForTesting;

/**
 * Created by JackNiu on 2019/7/7.
 */
public  final class TransientBlobKey  extends BlobKey {

    /**
     * Constructs a new BLOB key.
     */
    @VisibleForTesting
    public TransientBlobKey() {
        super(BlobType.TRANSIENT_BLOB);
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key
     *        the actual key data
     */
    TransientBlobKey(byte[] key) {
        super(BlobType.TRANSIENT_BLOB, key);
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key
     *        the actual key data
     * @param random
     *        the random component of the key
     */
    TransientBlobKey(byte[] key, byte[] random) {
        super(BlobType.TRANSIENT_BLOB, key, random);
    }
}
