package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.runtime.state.CheckpointStorageLocationReference;

import java.io.Serializable;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class CheckpointOptions implements Serializable {

    private static final long serialVersionUID = 5010126558083292915L;

    /** Type of the checkpoint. */
    private final CheckpointType checkpointType;

    /** Target location for the checkpoint. */
    private final CheckpointStorageLocationReference targetLocation;

    public CheckpointOptions(
            CheckpointType checkpointType,
            CheckpointStorageLocationReference targetLocation) {

        this.checkpointType = checkNotNull(checkpointType);
        this.targetLocation = checkNotNull(targetLocation);
    }

    // ------------------------------------------------------------------------

    /**
     * Returns the type of checkpoint to perform.
     */
    public CheckpointType getCheckpointType() {
        return checkpointType;
    }

    /**
     * Returns the target location for the checkpoint.
     */
    public CheckpointStorageLocationReference getTargetLocation() {
        return targetLocation;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return 31 * targetLocation.hashCode() + checkpointType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        else if (obj != null && obj.getClass() == CheckpointOptions.class) {
            final CheckpointOptions that = (CheckpointOptions) obj;
            return this.checkpointType == that.checkpointType &&
                    this.targetLocation.equals(that.targetLocation);
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "CheckpointOptions: " + checkpointType + " @ " + targetLocation;
    }

    // ------------------------------------------------------------------------
    //  Factory methods
    // ------------------------------------------------------------------------

    private static final CheckpointOptions CHECKPOINT_AT_DEFAULT_LOCATION =
            new CheckpointOptions(CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());

    public static CheckpointOptions forCheckpointWithDefaultLocation() {
        return CHECKPOINT_AT_DEFAULT_LOCATION;
    }
}
