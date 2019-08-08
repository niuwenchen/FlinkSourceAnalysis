package com.jackniu.flink.runtime.state;

/**
 * Created by JackNiu on 2019/7/5.
 */

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * CheckpointStorage implements the durable storage of checkpoint data and metadata streams.
 * An individual checkpoint or savepoint is stored to a {@link CheckpointStorageLocation},
 * created by this class.
 */
public interface CheckpointStorage {
    boolean supportsHighlyAvailableStorage();
    boolean hasDefaultSavepointLocation();
    CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException;
    CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException;
    CheckpointStorageLocation initializeLocationForSavepoint(
            long checkpointId,
            @Nullable String externalLocationPointer) throws IOException;

    CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId,
            CheckpointStorageLocationReference reference) throws IOException;

    CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException;
}
