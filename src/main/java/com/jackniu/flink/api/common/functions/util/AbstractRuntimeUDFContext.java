package com.jackniu.flink.api.common.functions.util;

import com.jackniu.flink.annotations.Internal;
import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.annotations.VisibleForTesting;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.TaskInfo;
import com.jackniu.flink.api.common.accumulators.*;
import com.jackniu.flink.api.common.accumulators.Histogram;
import com.jackniu.flink.api.common.cache.DistributedCache;
import com.jackniu.flink.api.common.functions.RuntimeContext;
import com.jackniu.flink.api.common.state.*;
import com.jackniu.flink.core.fs.Path;

import com.jackniu.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public  abstract  class AbstractRuntimeUDFContext implements RuntimeContext {

    private final TaskInfo taskInfo;

    private final ClassLoader userCodeClassLoader;

    private final ExecutionConfig executionConfig;

    private final Map<String, Accumulator<?, ?>> accumulators;

    private final DistributedCache distributedCache;

    private final MetricGroup metrics;

    public AbstractRuntimeUDFContext(TaskInfo taskInfo,
                                     ClassLoader userCodeClassLoader,
                                     ExecutionConfig executionConfig,
                                     Map<String, Accumulator<?, ?>> accumulators,
                                     Map<String, Future<Path>> cpTasks,
                                     MetricGroup metrics) {
        this.taskInfo = checkNotNull(taskInfo);
        this.userCodeClassLoader = userCodeClassLoader;
        this.executionConfig = executionConfig;
        this.distributedCache = new DistributedCache(checkNotNull(cpTasks));
        this.accumulators = checkNotNull(accumulators);
        this.metrics = metrics;
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public String getTaskName() {
        return taskInfo.getTaskName();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return taskInfo.getNumberOfParallelSubtasks();
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
        return taskInfo.getMaxNumberOfParallelSubtasks();
    }

    @Override
    public int getIndexOfThisSubtask() {
        return taskInfo.getIndexOfThisSubtask();
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metrics;
    }

    @Override
    public int getAttemptNumber() {
        return taskInfo.getAttemptNumber();
    }

    @Override
    public String getTaskNameWithSubtasks() {
        return taskInfo.getTaskNameWithSubtasks();
    }

    @Override
    public IntCounter getIntCounter(String name) {
        return (IntCounter) getAccumulator(name, IntCounter.class);
    }

    @Override
    public LongCounter getLongCounter(String name) {
        return (LongCounter) getAccumulator(name, LongCounter.class);
    }

    @Override
    public Histogram getHistogram(String name) {
        return (Histogram) getAccumulator(name, Histogram.class);
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        return (DoubleCounter) getAccumulator(name, DoubleCounter.class);
    }

    @Override
    public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
        if (accumulators.containsKey(name)) {
            throw new UnsupportedOperationException("The accumulator '" + name
                    + "' already exists and cannot be added.");
        }
        accumulators.put(name, accumulator);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
        return (Accumulator<V, A>) accumulators.get(name);
    }

    @Override
    public Map<String, Accumulator<?, ?>> getAllAccumulators() {
        return Collections.unmodifiableMap(this.accumulators);
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return this.userCodeClassLoader;
    }

    @Override
    public DistributedCache getDistributedCache() {
        return this.distributedCache;
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name,
                                                                         Class<? extends Accumulator<V, A>> accumulatorClass) {

        Accumulator<?, ?> accumulator = accumulators.get(name);

        if (accumulator != null) {
            AccumulatorHelper.compareAccumulatorTypes(name, accumulator.getClass(), accumulatorClass);
        } else {
            // Create new accumulator
            try {
                accumulator = accumulatorClass.newInstance();
            }
            catch (Exception e) {
                throw new RuntimeException("Cannot create accumulator " + accumulatorClass.getName());
            }
            accumulators.put(name, accumulator);
        }
        return (Accumulator<V, A>) accumulator;
    }

    @Override
    @PublicEvolving
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        throw new UnsupportedOperationException(
                "This state is only accessible by functions executed on a KeyedStream");
    }

    @Override
    @PublicEvolving
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        throw new UnsupportedOperationException(
                "This state is only accessible by functions executed on a KeyedStream");
    }

    @Override
    @PublicEvolving
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        throw new UnsupportedOperationException(
                "This state is only accessible by functions executed on a KeyedStream");
    }

    @Override
    @PublicEvolving
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        throw new UnsupportedOperationException(
                "This state is only accessible by functions executed on a KeyedStream");
    }

    @Override
    @PublicEvolving
    @Deprecated
    public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
        throw new UnsupportedOperationException(
                "This state is only accessible by functions executed on a KeyedStream");
    }

    @Override
    @PublicEvolving
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        throw new UnsupportedOperationException(
                "This state is only accessible by functions executed on a KeyedStream");
    }

    @Internal
    @VisibleForTesting
    public String getAllocationIDAsString() {
        return taskInfo.getAllocationIDAsString();
    }
}

