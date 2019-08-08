package com.jackniu.flink.runtime.state;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/5.
 * A storage location for one particular checkpoint, offering data persistent, metadata persistence,
 * and lifecycle/cleanup methods.
 */
public interface CheckpointStorageLocation extends CheckpointStreamFactory{
    CheckpointMetadataOutputStream createMetadataOutputStream() throws IOException;

    /**
     * Disposes the checkpoint location in case the checkpoint has failed.
     * This method disposes all the data at that location, not just the data written
     * by the particular node or process that calls this method.
     */
    void disposeOnFailure() throws IOException;

    /**
     * Gets a reference to the storage location. This reference is sent to the
     * target storage location via checkpoint RPC messages and checkpoint barriers,
     * in a format avoiding backend-specific classes.
     *
     * <p>If there is no custom location information that needs to be communicated,
     * this method can simply return {@link CheckpointStorageLocationReference#getDefault()}.
     */
    CheckpointStorageLocationReference getLocationReference();

}
