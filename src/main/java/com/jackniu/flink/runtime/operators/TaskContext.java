package com.jackniu.flink.runtime.operators;

/**
 * Created by JackNiu on 2019/7/6.
 */

import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializerFactory;
import com.jackniu.flink.runtime.io.disk.iomanager.IOManager;
import com.jackniu.flink.runtime.jobgraph.tasks.AbstractInvokable;
import com.jackniu.flink.runtime.memory.MemoryManager;
import com.jackniu.flink.runtime.metrics.groups.OperatorMetricGroup;
import com.jackniu.flink.runtime.operators.util.TaskConfig;
import com.jackniu.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import com.jackniu.flink.util.Collector;
import com.jackniu.flink.util.MutableObjectIterator;

/**
 * The task context gives a driver (e.g., {@link }, or {@link }) access to
 * the runtime components and configuration that they can use to fulfil their task.
 *
 * @param <S> The UDF type.
 * @param <OT> The produced data type.
 *
 * @see Driver
 */
public interface TaskContext<S, OT>  {
    TaskConfig getTaskConfig();

    TaskManagerRuntimeInfo getTaskManagerInfo();

    ClassLoader getUserCodeClassLoader();

    MemoryManager getMemoryManager();

    IOManager getIOManager();

    <X> MutableObjectIterator<X> getInput(int index);

    <X> TypeSerializerFactory<X> getInputSerializer(int index);

    <X> TypeComparator<X> getDriverComparator(int index);

    S getStub();

    ExecutionConfig getExecutionConfig();

    Collector<OT> getOutputCollector();

    AbstractInvokable getContainingTask();

    String formatLogString(String message);

    OperatorMetricGroup getMetricGroup();

}
