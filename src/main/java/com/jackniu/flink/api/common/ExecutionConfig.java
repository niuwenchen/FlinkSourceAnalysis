package com.jackniu.flink.api.common;

/**
 * Created by JackNiu on 2019/6/6.
 */

import com.esotericsoftware.kryo.Serializer;
import com.jackniu.flink.annotations.Internal;
import com.jackniu.flink.api.common.restartstrategy.RestartStrategies;
import com.jackniu.flink.configuration.MetricOptions;
import com.jackniu.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;

import static com.jackniu.flink.util.Preconditions.checkArgument;

/**
 * 定义程序执行的配置
 * The default parallelism of the program 默认并行度
 * The number of retries in the case of failed executions.
 * The delay between execution retries.
 * ExecutionMode  of the program: Batch or Pipelined.The default execution mode is ExecutionMode#PIPELINED
 * "closure cleaner" ，it removes unused references to the enclosing class
 * register types and serializers to increase the efficiency of  handling
 *         Enable hinting/optimizing or disable the "static code analyzer".静态代码检查 pre-interprets user-defined functions
 */

public class ExecutionConfig implements Serializable, Archiveable<ArchivedExecutionConfig> {
    private static final long serialVersionUID = 1L;

    @Deprecated
    public static final int PARALLELISM_AUTO_MAX = Integer.MAX_VALUE;

    public static final int PARALLELISM_DEFAULT = -1;
    public static final int PARALLELISM_UNKNOWN = -2;

    private static final long DEFAULT_RESTART_DELAY = 10000L;

    /** Defines how data exchange happens - batch or pipelined */
    private ExecutionMode executionMode = ExecutionMode.PIPELINED;

    private boolean useClosureCleaner = true;

    private int parallelism = PARALLELISM_DEFAULT;

    private int maxParallelism = -1;
    @Deprecated
    private int numberOfExecutionRetries = -1;

    private boolean forceKryo = false;

    /** Flag to indicate whether generic types (through Kryo) are supported */
    private boolean disableGenericTypes = false;

    private boolean objectReuse = false;

    private boolean autoTypeRegistrationEnabled = true;

    private boolean forceAvro = false;

    private CodeAnalysisMode codeAnalysisMode = CodeAnalysisMode.DISABLE;

    /** If set to true, progress updates are printed to System.out during execution */
    private boolean printProgressDuringExecution = true;

    private long autoWatermarkInterval = 0;

    /**
     * Interval in milliseconds for sending latency tracking marks from the sources to the sinks.
     */
    private long latencyTrackingInterval = MetricOptions.LATENCY_INTERVAL.defaultValue();

    private boolean isLatencyTrackingConfigured = false;

    /**
     * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
     */
    @Deprecated
    private long executionRetryDelay = DEFAULT_RESTART_DELAY;

    private RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
            new RestartStrategies.FallbackRestartStrategyConfiguration();

    private long taskCancellationIntervalMillis = -1;

    //超时之后，正在进行的任务取消将导致致命的TaskManager错误，通常会杀死JVM。
    private long taskCancellationTimeoutMillis = -1;
    private boolean useSnapshotCompression = false;
    private boolean failTaskOnCheckpointError = true;



    // ------------------------------- User code values --------------------------------------------
    private GlobalJobParameters globalJobParameters;

    //在Kryo和PojoSerializer中注册的序列化器和类型
    private LinkedHashMap<Class<?>, SerializableSerializer<?>> registeredTypesWithKryoSerializers = new LinkedHashMap<Class<?>, SerializableSerializer<?>>();

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> registeredTypesWithKryoSerializerClasses = new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();

    private LinkedHashMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers = new LinkedHashMap<Class<?>, SerializableSerializer<?>>();

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses = new LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>();

    private LinkedHashSet<Class<?>> registeredKryoTypes = new LinkedHashSet<Class<?>>();

    private LinkedHashSet<Class<?>> registeredPojoTypes = new LinkedHashSet<Class<?>>();


    //这将分析用户代码函数，并将未使用的字段设置为null。在大多数情况下，这将使闭包或匿名内部类可序列化，而由于Scala或Java实现构件的原因，这些类无法序列化。用户代码必须是可序列化的，因为它需要发送到工作节点。
    public ExecutionConfig enableClosureCleaner() {
        useClosureCleaner = true;
        return this;
    }

    public ExecutionConfig disableClosureCleaner() {
        useClosureCleaner = false;
        return this;
    }
    public boolean isClosureCleanerEnabled() {
        return useClosureCleaner;
    }

    //设置自动watermark发射间隔，整个流媒体系统都使用水印来跟踪时间的进程，例如，在基于时间的窗口中使用
    public ExecutionConfig setAutoWatermarkInterval(long interval) {
        this.autoWatermarkInterval = interval;
        return this;
    }
    public long getAutoWatermarkInterval()  {
        return this.autoWatermarkInterval;
    }


    //从源到接收器发送延迟跟踪标记的间隔。
    // Flink将在指定的时间间隔从源发送延迟跟踪标记。
    public ExecutionConfig setLatencyTrackingInterval(long interval) {
        this.latencyTrackingInterval = interval;
        this.isLatencyTrackingConfigured = true;
        return this;
    }
    public long getLatencyTrackingInterval() {
        return latencyTrackingInterval;
    }
    public boolean isLatencyTrackingEnabled() {
        return isLatencyTrackingConfigured && latencyTrackingInterval > 0;
    }
    public boolean isLatencyTrackingConfigured() {
        return isLatencyTrackingConfigured;
    }

    public int getParallelism() {
        return parallelism;
    }
    public ExecutionConfig setParallelism(int parallelism) {
        if (parallelism != PARALLELISM_UNKNOWN) {
            if (parallelism < 1 && parallelism != PARALLELISM_DEFAULT) {
                throw new IllegalArgumentException(
                        "Parallelism must be at least one, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");
            }
            this.parallelism = parallelism;
        }
        return this;
    }
    public int getMaxParallelism() {
        return maxParallelism;
    }

    public void setMaxParallelism(int maxParallelism) {
        checkArgument(maxParallelism > 0, "The maximum parallelism must be greater than 0.");
        this.maxParallelism = maxParallelism;
    }

    /**
     * Gets the interval (in milliseconds) between consecutive attempts to cancel a running task.
     */
    public long getTaskCancellationInterval() {
        return this.taskCancellationIntervalMillis;
    }

    public ExecutionConfig setTaskCancellationInterval(long interval) {
        this.taskCancellationIntervalMillis = interval;
        return this;
    }

    public long getTaskCancellationTimeout() {
        return this.taskCancellationTimeoutMillis;
    }
    public ExecutionConfig setTaskCancellationTimeout(long timeout) {
        checkArgument(timeout >= 0, "Timeout needs to be >= 0.");
        this.taskCancellationTimeoutMillis = timeout;
        return this;
    }

    // 设置恢复时候的重启策略
    // ExecutionConfig config = env.getConfig();
    // config.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,1000));

    public void setRestartStrategy(RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
        this.restartStrategyConfiguration = Preconditions.checkNotNull(restartStrategyConfiguration);
    }

    @Deprecated
    public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
        if (restartStrategyConfiguration instanceof RestartStrategies.FallbackRestartStrategyConfiguration) {
            // support the old API calls by creating a restart strategy from them
            if (getNumberOfExecutionRetries() > 0 && getExecutionRetryDelay() >= 0) {
                return RestartStrategies.fixedDelayRestart(getNumberOfExecutionRetries(), getExecutionRetryDelay());
            } else if (getNumberOfExecutionRetries() == 0) {
                return RestartStrategies.noRestart();
            } else {
                return restartStrategyConfiguration;
            }
        } else {
            return restartStrategyConfiguration;
        }
    }
    @Deprecated
    public int getNumberOfExecutionRetries() {
        return numberOfExecutionRetries;
    }
    @Deprecated
    public long getExecutionRetryDelay() {
        return executionRetryDelay;
    }
    @Deprecated
    public ExecutionConfig setNumberOfExecutionRetries(int numberOfExecutionRetries) {
        if (numberOfExecutionRetries < -1) {
            throw new IllegalArgumentException(
                    "The number of execution retries must be non-negative, or -1 (use system default)");
        }
        this.numberOfExecutionRetries = numberOfExecutionRetries;
        return this;
    }
    @Deprecated
    public ExecutionConfig setExecutionRetryDelay(long executionRetryDelay) {
        if (executionRetryDelay < 0 ) {
            throw new IllegalArgumentException(
                    "The delay between retries must be non-negative.");
        }
        this.executionRetryDelay = executionRetryDelay;
        return this;
    }


    public void setExecutionMode(ExecutionMode executionMode) {
        this.executionMode = executionMode;
    }
    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public void enableForceKryo() {
        forceKryo = true;
    }
    public void disableForceKryo() {
        forceKryo = false;
    }

    public boolean isForceKryoEnabled() {
        return forceKryo;
    }

    public void enableGenericTypes() {
        disableGenericTypes = false;
    }

    public void disableGenericTypes() {
        disableGenericTypes = true;
    }

    public boolean hasGenericTypesDisabled() {
        return disableGenericTypes;
    }

    public void enableForceAvro() {
        forceAvro = true;
    }

    public void disableForceAvro() {
        forceAvro = false;
    }

    public boolean isForceAvroEnabled() {
        return forceAvro;
    }

    public ExecutionConfig enableObjectReuse() {
        objectReuse = true;
        return this;
    }
    public ExecutionConfig disableObjectReuse() {
        objectReuse = false;
        return this;
    }

    public boolean isObjectReuseEnabled() {
        return objectReuse;
    }

    public void setCodeAnalysisMode(CodeAnalysisMode codeAnalysisMode) {
        this.codeAnalysisMode = codeAnalysisMode;
    }

    public CodeAnalysisMode getCodeAnalysisMode() {
        return codeAnalysisMode;
    }

    public ExecutionConfig enableSysoutLogging() {
        this.printProgressDuringExecution = true;
        return this;
    }

    public ExecutionConfig disableSysoutLogging() {
        this.printProgressDuringExecution = false;
        return this;
    }

    public boolean isSysoutLoggingEnabled() {
        return this.printProgressDuringExecution;
    }

    public GlobalJobParameters getGlobalJobParameters() {
        return globalJobParameters;
    }

    public void setGlobalJobParameters(GlobalJobParameters globalJobParameters) {
        this.globalJobParameters = globalJobParameters;
    }

    public <T extends Serializer<?> & Serializable>void addDefaultKryoSerializer(Class<?> type, T serializer) {
        if (type == null || serializer == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        defaultKryoSerializers.put(type, new SerializableSerializer(serializer));
    }
    public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
        if (type == null || serializerClass == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }
        defaultKryoSerializerClasses.put(type, serializerClass);
    }

    public <T extends Serializer<?> & Serializable>void registerTypeWithKryoSerializer(Class<?> type, T serializer) {
        if (type == null || serializer == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        registeredTypesWithKryoSerializers.put(type, new SerializableSerializer(serializer));
    }

    @SuppressWarnings("rawtypes")
    public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer> serializerClass) {
        if (type == null || serializerClass == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        @SuppressWarnings("unchecked")
        Class<? extends Serializer<?>> castedSerializerClass = (Class<? extends Serializer<?>>) serializerClass;
        registeredTypesWithKryoSerializerClasses.put(type, castedSerializerClass);
    }
    public void registerPojoType(Class<?> type) {
        if (type == null) {
            throw new NullPointerException("Cannot register null type class.");
        }
        if (!registeredPojoTypes.contains(type)) {
            registeredPojoTypes.add(type);
        }
    }
    public void registerKryoType(Class<?> type) {
        if (type == null) {
            throw new NullPointerException("Cannot register null type class.");
        }
        registeredKryoTypes.add(type);
    }
    public LinkedHashMap<Class<?>, SerializableSerializer<?>> getRegisteredTypesWithKryoSerializers() {
        return registeredTypesWithKryoSerializers;
    }

    public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> getRegisteredTypesWithKryoSerializerClasses() {
        return registeredTypesWithKryoSerializerClasses;
    }

    public LinkedHashMap<Class<?>, SerializableSerializer<?>> getDefaultKryoSerializers() {
        return defaultKryoSerializers;
    }

    /**
     * Returns the registered default Kryo Serializer classes.
     */
    public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> getDefaultKryoSerializerClasses() {
        return defaultKryoSerializerClasses;
    }

    public LinkedHashSet<Class<?>> getRegisteredKryoTypes() {
        if (isForceKryoEnabled()) {
            // if we force kryo, we must also return all the types that
            // were previously only registered as POJO
            LinkedHashSet<Class<?>> result = new LinkedHashSet();
            result.addAll(registeredKryoTypes);
            for(Class<?> t : registeredPojoTypes) {
                if (!result.contains(t)) {
                    result.add(t);
                }
            }
            return result;
        } else {
            return registeredKryoTypes;
        }
    }

    /**
     * Returns the registered POJO types.
     */
    public LinkedHashSet<Class<?>> getRegisteredPojoTypes() {
        return registeredPojoTypes;
    }


    public boolean isAutoTypeRegistrationDisabled() {
        return !autoTypeRegistrationEnabled;
    }


    public void disableAutoTypeRegistration() {
        this.autoTypeRegistrationEnabled = false;
    }

    public boolean isUseSnapshotCompression() {
        return useSnapshotCompression;
    }

    public void setUseSnapshotCompression(boolean useSnapshotCompression) {
        this.useSnapshotCompression = useSnapshotCompression;
    }

    @Internal
    public boolean isFailTaskOnCheckpointError() {
        return failTaskOnCheckpointError;
    }

    /**
     * This method is visible because of the way the configuration is currently forwarded from the checkpoint config to
     * the task. This should not be called by the user, please use CheckpointConfig.setFailOnCheckpointingErrors(...)
     * instead.
     */
    @Internal
    public void setFailTaskOnCheckpointError(boolean failTaskOnCheckpointError) {
        this.failTaskOnCheckpointError = failTaskOnCheckpointError;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutionConfig) {
            ExecutionConfig other = (ExecutionConfig) obj;

            return other.canEqual(this) &&
                    Objects.equals(executionMode, other.executionMode) &&
                    useClosureCleaner == other.useClosureCleaner &&
                    parallelism == other.parallelism &&
                    ((restartStrategyConfiguration == null && other.restartStrategyConfiguration == null) ||
                            (null != restartStrategyConfiguration && restartStrategyConfiguration.equals(other.restartStrategyConfiguration))) &&
                    forceKryo == other.forceKryo &&
                    disableGenericTypes == other.disableGenericTypes &&
                    objectReuse == other.objectReuse &&
                    autoTypeRegistrationEnabled == other.autoTypeRegistrationEnabled &&
                    forceAvro == other.forceAvro &&
                    Objects.equals(codeAnalysisMode, other.codeAnalysisMode) &&
                    printProgressDuringExecution == other.printProgressDuringExecution &&
                    Objects.equals(globalJobParameters, other.globalJobParameters) &&
                    autoWatermarkInterval == other.autoWatermarkInterval &&
                    registeredTypesWithKryoSerializerClasses.equals(other.registeredTypesWithKryoSerializerClasses) &&
                    defaultKryoSerializerClasses.equals(other.defaultKryoSerializerClasses) &&
                    registeredKryoTypes.equals(other.registeredKryoTypes) &&
                    registeredPojoTypes.equals(other.registeredPojoTypes) &&
                    taskCancellationIntervalMillis == other.taskCancellationIntervalMillis &&
                    useSnapshotCompression == other.useSnapshotCompression;

        } else {
            return false;
        }
    }


    public int hashCode() {
        return Objects.hash(
                executionMode,
                useClosureCleaner,
                parallelism,
                restartStrategyConfiguration,
                forceKryo,
                disableGenericTypes,
                objectReuse,
                autoTypeRegistrationEnabled,
                forceAvro,
                codeAnalysisMode,
                printProgressDuringExecution,
                globalJobParameters,
                autoWatermarkInterval,
                registeredTypesWithKryoSerializerClasses,
                defaultKryoSerializerClasses,
                registeredKryoTypes,
                registeredPojoTypes,
                taskCancellationIntervalMillis,
                useSnapshotCompression);
    }

    public boolean canEqual(Object obj) {
        return obj instanceof ExecutionConfig;
    }




    public ArchivedExecutionConfig archive() {
        return null;
    }




    // ------------------------------ Utilities  ----------------------------------

    public static class SerializableSerializer<T extends Serializer<?> & Serializable> implements Serializable {
        private static final long serialVersionUID = 4687893502781067189L;

        private T serializer;

        public SerializableSerializer(T serializer) {
            this.serializer = serializer;
        }

        public T getSerializer() {
            return serializer;
        }
    }

    /**
     * 可以在这里加载用户的配置
     * Abstract class for a custom user configuration object registered at the execution config.
     *
     * This user config is accessible at runtime through
     * getRuntimeContext().getExecutionConfig().GlobalJobParameters()
     */
    public static class GlobalJobParameters implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * Convert UserConfig into a {@code Map<String, String>} representation.
         * This can be used by the runtime, for example for presenting the user config in the web frontend.
         *
         * @return Key/Value representation of the UserConfig
         */
        public Map<String, String> toMap() {
            return Collections.emptyMap();
        }
    }

}
