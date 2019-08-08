package com.jackniu.flink.runtime.state;

/**
 * Created by JackNiu on 2019/7/7.
 */
/**
 * This interface must be implemented by functions/operations that want to receive
 * a commit notification once a checkpoint has been completely acknowledged by all
 * participants.
 */
public interface CheckpointListener {
    /**
     * This method is called as a notification once a distributed checkpoint has been completed.
     *
     * Note that any exception during this method will not cause the checkpoint to
     * fail any more.
     *
     * @param checkpointId The ID of the checkpoint that has been completed.
     * @throws Exception
     */
    void notifyCheckpointComplete(long checkpointId) throws Exception;

}
