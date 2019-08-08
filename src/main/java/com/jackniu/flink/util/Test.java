package com.jackniu.flink.util;

import com.jackniu.flink.runtime.jobgraph.JobVertexID;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class Test {
    public static void main(String[] args) {
        JobVertexID jd = new JobVertexID(1000L,2000L);
        System.out.println(jd.toString());
    }
}
