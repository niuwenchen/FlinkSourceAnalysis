package com.jackniu.flink.runtime.jobmanager;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class SubmittedJobGraph implements Serializable {

    private static final long serialVersionUID = 2836099271734771825L;

    /** The submitted {@link JobGraph} */
    private final JobGraph jobGraph;

    /** The {@link JobInfo}. */
    private final JobInfo jobInfo;

    /**
     * Creates a {@link SubmittedJobGraph}.
     *
     * @param jobGraph The submitted {@link JobGraph}
     * @param jobInfo  The {@link JobInfo}
     */
    public SubmittedJobGraph(JobGraph jobGraph, @Nullable JobInfo jobInfo) {
        this.jobGraph = checkNotNull(jobGraph, "Job graph");
        this.jobInfo = jobInfo;
    }

    /**
     * Returns the submitted {@link JobGraph}.
     */
    public JobGraph getJobGraph() {
        return jobGraph;
    }

    /**
     * Returns the {@link JobID} of the submitted {@link JobGraph}.
     */
    public JobID getJobId() {
        return jobGraph.getJobID();
    }

    /**
     * Returns the {@link JobInfo} of the client who submitted the {@link JobGraph}.
     */
    public JobInfo getJobInfo() throws Exception {
        return jobInfo;
    }

    @Override
    public String toString() {
        return String.format("SubmittedJobGraph(%s, %s)", jobGraph.getJobID(), jobInfo);
    }
}

