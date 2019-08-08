package com.jackniu.flink.runtime.highavailability;

import com.jackniu.flink.api.common.JobID;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface RunningJobsRegistry {
    enum JobSchedulingStatus {

        /** Job has not been scheduled, yet. */
        PENDING,

        /** Job has been scheduled and is not yet finished. */
        RUNNING,

        /** Job has been finished, successfully or unsuccessfully. */
        DONE;
    }
    void setJobRunning(JobID jobID) throws IOException;

    JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException;

    void clearJob(JobID jobID) throws IOException;

}
