package com.jackniu.flink.runtime.taskmanager;

import com.jackniu.flink.configuration.Configuration;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface TaskManagerRuntimeInfo {
    /**
     * Gets the configuration that the TaskManager was started with.
     *
     * @return The configuration that the TaskManager was started with.
     */
    Configuration getConfiguration();

    /**
     * Gets the list of temporary file directories.
     *
     * @return The list of temporary file directories.
     */
    String[] getTmpDirectories();

    /**
     * Checks whether the TaskManager should exit the JVM when the task thread throws
     * an OutOfMemoryError.
     *
     * @return True to terminate the JVM on an OutOfMemoryError, false otherwise.
     */
    boolean shouldExitJvmOnOutOfMemoryError();

}
