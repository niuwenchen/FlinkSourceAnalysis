package com.jackniu.flink.streaming.api.environment;

import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.streaming.api.TimeCharacteristic;
import com.jackniu.flink.streaming.api.transformations.StreamTransformation;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JackNiu on 2019/6/6.
 */
public abstract class StreamExecutionEnvironment {
    public static final String DEFAULT_JOB_NAME = "Flink Streaming Job";
    private static final TimeCharacteristic DEFAULT_TIME_CHARACTERISTIC;
    private static final long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L;
    private static StreamExecutionEnvironmentFactory contextEnvironmentFactory;
    private static int defaultLocalParallelism;
    private final ExecutionConfig config = new ExecutionConfig();
    private final CheckpointConfig checkpointCfg = new CheckpointConfig();

    protected final List<StreamTransformation<?>> transformations = new ArrayList<>();

    private long bufferTimeout = DEFAULT_NETWORK_BUFFER_TIMEOUT;

    protected boolean isChainingEnabled = true;

    /** The state backend used for storing k/v state and state snapshots. */
    private StateBackend defaultStateBackend;

    /** The time characteristic used by the data streams. */
    private TimeCharacteristic timeCharacteristic = DEFAULT_TIME_CHARACTERISTIC;

    protected final List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cacheFile = new ArrayList<>();


//
//    public StreamExecutionEnvironment() {
//        this.timeCharacteristic = DEFAULT_TIME_CHARACTERISTIC;
//        this.cacheFile = new ArrayList();
//    }




    static {
        DEFAULT_TIME_CHARACTERISTIC = TimeCharacteristic.ProcessingTime;
        defaultLocalParallelism = Runtime.getRuntime().availableProcessors();
    }
}
