package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.api.common.JobID;
import com.jackniu.flink.api.java.tuple.Tuple2;
import com.jackniu.flink.runtime.jobgraph.JobStatus;
import com.jackniu.flink.runtime.jobgraph.OperatorID;
import com.jackniu.flink.runtime.state.CompletedCheckpointStorageLocation;
import com.jackniu.flink.runtime.state.SharedStateRegistry;
import com.jackniu.flink.runtime.state.StateUtil;
import com.jackniu.flink.runtime.state.StreamStateHandle;
import com.jackniu.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

import static com.jackniu.flink.util.Preconditions.checkArgument;
import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class CompletedCheckpoint  implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CompletedCheckpoint.class);

    private static final long serialVersionUID = -8360248179615702014L;

    // ------------------------------------------------------------------------

    /**
     * The ID of the job that the checkpoint belongs to.
     */
    private final JobID job;

    /**
     * The ID (logical timestamp) of the checkpoint.
     */
    private final long checkpointID;

    /**
     * The timestamp when the checkpoint was triggered.
     */
    private final long timestamp;

    /**
     * The duration of the checkpoint (completion timestamp - trigger timestamp).
     */
    private final long duration;

    /**
     * States of the different operator groups belonging to this checkpoint.
     */
    private final Map<OperatorID, OperatorState> operatorStates;

    /**
     * Properties for this checkpoint.
     */
    private final CheckpointProperties props;

    /**
     * States that were created by a hook on the master (in the checkpoint coordinator).
     */
    private final Collection<MasterState> masterHookStates;

    /**
     * The location where the checkpoint is stored.
     */
    private final CompletedCheckpointStorageLocation storageLocation;

    /**
     * The state handle to the externalized meta data.
     */
    private final StreamStateHandle metadataHandle;

    /**
     * External pointer to the completed checkpoint (for example file path).
     */
    private final String externalPointer;

    /**
     * Optional stats tracker callback for discard.
     */
    @Nullable
    private transient volatile CompletedCheckpointStats.DiscardCallback discardCallback;

    // ------------------------------------------------------------------------

    public CompletedCheckpoint(
            JobID job,
            long checkpointID,
            long timestamp,
            long completionTimestamp,
            Map<OperatorID, OperatorState> operatorStates,
            @Nullable Collection<MasterState> masterHookStates,
            CheckpointProperties props,
            CompletedCheckpointStorageLocation storageLocation) {

        checkArgument(checkpointID >= 0);
        checkArgument(timestamp >= 0);
        checkArgument(completionTimestamp >= 0);

        this.job = checkNotNull(job);
        this.checkpointID = checkpointID;
        this.timestamp = timestamp;
        this.duration = completionTimestamp - timestamp;

        // we create copies here, to make sure we have no shared mutable
        // data structure with the "outside world"
        this.operatorStates = new HashMap<>(checkNotNull(operatorStates));
        this.masterHookStates = masterHookStates == null || masterHookStates.isEmpty() ?
                Collections.emptyList() :
                new ArrayList<>(masterHookStates);

        this.props = checkNotNull(props);
        this.storageLocation = checkNotNull(storageLocation);
        this.metadataHandle = storageLocation.getMetadataHandle();
        this.externalPointer = storageLocation.getExternalPointer();
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return job;
    }

    public long getCheckpointID() {
        return checkpointID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getDuration() {
        return duration;
    }

    public CheckpointProperties getProperties() {
        return props;
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public Collection<MasterState> getMasterHookStates() {
        return Collections.unmodifiableCollection(masterHookStates);
    }

    public StreamStateHandle getMetadataHandle() {
        return metadataHandle;
    }

    public String getExternalPointer() {
        return externalPointer;
    }

    public long getStateSize() {
        long result = 0L;

        for (OperatorState operatorState : operatorStates.values()) {
            result += operatorState.getStateSize();
        }

        return result;
    }

    // ------------------------------------------------------------------------
    //  Shared State
    // ------------------------------------------------------------------------

    /**
     * Register all shared states in the given registry. This is method is called
     * before the checkpoint is added into the store.
     *
     * @param sharedStateRegistry The registry where shared states are registered
     */
    public void registerSharedStatesAfterRestored(SharedStateRegistry sharedStateRegistry) {
        sharedStateRegistry.registerAll(operatorStates.values());
    }

    // ------------------------------------------------------------------------
    //  Discard and Dispose
    // ------------------------------------------------------------------------

    public void discardOnFailedStoring() throws Exception {
        doDiscard();
    }

    public boolean discardOnSubsume() throws Exception {
        if (props.discardOnSubsumed()) {
            doDiscard();
            return true;
        }

        return false;
    }

    public boolean discardOnShutdown(JobStatus jobStatus) throws Exception {

        if (jobStatus == JobStatus.FINISHED && props.discardOnJobFinished() ||
                jobStatus == JobStatus.CANCELED && props.discardOnJobCancelled() ||
                jobStatus == JobStatus.FAILED && props.discardOnJobFailed() ||
                jobStatus == JobStatus.SUSPENDED && props.discardOnJobSuspended()) {

            doDiscard();
            return true;
        } else {
            LOG.info("Checkpoint with ID {} at '{}' not discarded.", checkpointID, externalPointer);
            return false;
        }
    }

    private void doDiscard() throws Exception {
        LOG.trace("Executing discard procedure for {}.", this);

        try {
            // collect exceptions and continue cleanup
            Exception exception = null;

            // drop the metadata
            try {
                metadataHandle.discardState();
            } catch (Exception e) {
                exception = e;
            }

            // discard private state objects
            try {
                StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            // discard location as a whole
            try {
                storageLocation.disposeStorageLocation();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            if (exception != null) {
                throw exception;
            }
        } finally {
            operatorStates.clear();

            // to be null-pointer safe, copy reference to stack
            CompletedCheckpointStats.DiscardCallback discardCallback = this.discardCallback;
            if (discardCallback != null) {
                discardCallback.notifyDiscardedCheckpoint();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    public static boolean checkpointsMatch(
            Collection<CompletedCheckpoint> first,
            Collection<CompletedCheckpoint> second) {
        if (first.size() != second.size()) {
            return false;
        }

        List<Tuple2<Long, JobID>> firstInterestingFields = new ArrayList<>(first.size());

        for (CompletedCheckpoint checkpoint : first) {
            firstInterestingFields.add(
                    new Tuple2<>(checkpoint.getCheckpointID(), checkpoint.getJobId()));
        }

        List<Tuple2<Long, JobID>> secondInterestingFields = new ArrayList<>(second.size());

        for (CompletedCheckpoint checkpoint : second) {
            secondInterestingFields.add(
                    new Tuple2<>(checkpoint.getCheckpointID(), checkpoint.getJobId()));
        }

        return firstInterestingFields.equals(secondInterestingFields);
    }

    /**
     * Sets the callback for tracking when this checkpoint is discarded.
     *
     * @param discardCallback Callback to call when the checkpoint is discarded.
     */
    void setDiscardCallback(@Nullable CompletedCheckpointStats.DiscardCallback discardCallback) {
        this.discardCallback = discardCallback;
    }

    @Override
    public String toString() {
        return String.format("Checkpoint %d @ %d for %s", checkpointID, timestamp, job);

    }
}
