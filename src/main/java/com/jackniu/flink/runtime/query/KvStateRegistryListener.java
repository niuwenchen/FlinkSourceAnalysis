package com.jackniu.flink.runtime.query;

import com.jackniu.flink.api.common.JobID;
import com.jackniu.flink.queryablestate.KvStateID;
import com.jackniu.flink.runtime.jobgraph.JobVertexID;
import com.jackniu.flink.runtime.state.KeyGroupRange;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface KvStateRegistryListener {
    /**
            * Notifies the listener about a registered KvState instance.
            *
            * @param jobId            Job ID the KvState instance belongs to
	 * @param jobVertexId      JobVertexID the KvState instance belongs to
	 * @param keyGroupRange    Key group range the KvState instance belongs to
	 * @param registrationName Name under which the KvState is registered
	 * @param kvStateId        ID of the KvState instance
	 */
    void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId);

    /**
     * Notifies the listener about an unregistered KvState instance.
     *
     * @param jobId            Job ID the KvState instance belongs to
     * @param jobVertexId      JobVertexID the KvState instance belongs to
     * @param keyGroupRange    Key group range the KvState instance belongs to
     * @param registrationName Name under which the KvState is registered
     */
    void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName);

}
