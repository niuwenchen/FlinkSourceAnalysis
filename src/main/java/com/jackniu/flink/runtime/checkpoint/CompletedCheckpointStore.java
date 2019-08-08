package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.runtime.jobgraph.JobStatus;
import javafx.print.PrinterJob;

import java.util.List;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface CompletedCheckpointStore {
    /**
     * Recover available {@link CompletedCheckpoint} instances.
     *
     * <p>After a call to this method, {@link #getLatestCheckpoint()} returns the latest
     * available checkpoint.
     */
    void recover() throws Exception;

    /**
     * Adds a {@link CompletedCheckpoint} instance to the list of completed checkpoints.
     *
     * <p>Only a bounded number of checkpoints is kept. When exceeding the maximum number of
     * retained checkpoints, the oldest one will be discarded.
     */
    void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception;

    /**
     * Returns the latest {@link CompletedCheckpoint} instance or <code>null</code> if none was
     * added.
     */
    CompletedCheckpoint getLatestCheckpoint() throws Exception;

    /**
     * Shuts down the store.
     *
     * <p>The job status is forwarded and used to decide whether state should
     * actually be discarded or kept.
     *
     * @param jobStatus Job state on shut down
     */
    void shutdown(JobStatus jobStatus) throws Exception;

    /**
     * Returns all {@link CompletedCheckpoint} instances.
     *
     * <p>Returns an empty list if no checkpoint has been added yet.
     */
    List<CompletedCheckpoint> getAllCheckpoints() throws Exception;

    /**
     * Returns the current number of retained checkpoints.
     */
    int getNumberOfRetainedCheckpoints();

    /**
     * Returns the max number of retained checkpoints.
     */
    int getMaxNumberOfRetainedCheckpoints();

    /**
     * This method returns whether the completed checkpoint store requires checkpoints to be
     * externalized. Externalized checkpoints have their meta data persisted, which the checkpoint
     * store can exploit (for example by simply pointing the persisted metadata).
     *
     * @return True, if the store requires that checkpoints are externalized before being added, false
     *         if the store stores the metadata itself.
     */
    boolean requiresExternalizedCheckpoints();
}
