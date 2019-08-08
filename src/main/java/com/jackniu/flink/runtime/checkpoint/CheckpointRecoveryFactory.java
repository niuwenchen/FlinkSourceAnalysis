package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.api.common.JobID;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface CheckpointRecoveryFactory {
    /**
     * Creates a {@link CompletedCheckpointStore} instance for a job.
     *
     * @param jobId           Job ID to recover checkpoints for
     * @param maxNumberOfCheckpointsToRetain Maximum number of checkpoints to retain
     * @param userClassLoader User code class loader of the job
     * @return {@link CompletedCheckpointStore} instance for the job
     */
    CompletedCheckpointStore createCheckpointStore(JobID jobId, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader)
            throws Exception;

    /**
     * Creates a {@link CheckpointIDCounter} instance for a job.
     *
     * @param jobId Job ID to recover checkpoints for
     * @return {@link CheckpointIDCounter} instance for the job
     */
    CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception;
}
