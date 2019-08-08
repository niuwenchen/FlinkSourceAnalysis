package com.jackniu.flink.runtime.jobgraph;

/**
 * Created by JackNiu on 2019/7/6.
 */
public enum DistributionPattern {
    //Each producing sub task is connected to each sub task of the consuming task.
    ALL_TO_ALL,
        //Each producing sub task is connected to one or more subtask(s) of the consuming task.
    POINTWISE
}
