package com.jackniu.flink.api.common;

/**
 * Created by JackNiu on 2019/6/6.
 */

import com.sun.istack.internal.Interned;

import java.io.Serializable;
import java.util.Map;

/**
 * Serializable class which is created when archiving the job.
 * 可以被用来显示任务信息
 */

public class ArchivedExecutionConfig implements Serializable {
    private static final long serialVersionUID = 2126156250920316528L;

    private final String executionMode;

    public ArchivedExecutionConfig(ExecutionConfig ec){
        executionMode = ec.getExecutionMode().name();
    }

    public String getExecutionMode(){ return executionMode; }

}
