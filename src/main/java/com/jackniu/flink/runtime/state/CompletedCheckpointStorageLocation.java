package com.jackniu.flink.runtime.state;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/5.
 */
public interface CompletedCheckpointStorageLocation  extends java.io.Serializable {
    /**
     * Gets the external pointer to the checkpoint. The pointer can be used to resume
     * a program from the savepoint or checkpoint, and is typically passed as a command
     * line argument, an HTTP request parameter, or stored in a system like ZooKeeper.
     */
    String getExternalPointer();

    /**
     * Gets the state handle to the checkpoint's metadata.
     */
    StreamStateHandle getMetadataHandle();

    /**
     * Disposes the storage location. This method should be called after all state objects have
     * been released. It typically disposes the base structure of the checkpoint storage,
     * like the checkpoint directory.
     */
    void disposeStorageLocation() throws IOException;
}
