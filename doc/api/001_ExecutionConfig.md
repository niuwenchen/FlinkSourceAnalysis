## ExecutionConfig

    引用来源: 
        com.jackniu.flink.streaming.api.environment.StreamExecutionEnvironment
        private final ExecutionConfig config = new ExecutionConfig();
        
        ExecutionConfig implements Serializable, Archiveable<ArchivedExecutionConfig>
            
     * 定义程序执行的配置
     * The default parallelism of the program 默认并行度
     * The number of retries in the case of failed executions.
     * The delay between execution retries.
     * ExecutionMode  of the program: Batch or Pipelined.The default execution mode is ExecutionMode#PIPELINED
     * "closure cleaner" ，it removes unused references to the enclosing class
     * register types and serializers to increase the efficiency of  handling
     *         Enable hinting/optimizing or disable the "static code analyzer".静态代码检查 pre-interprets user-defined functions
     

* PARALLELISM_AUTO_MAX=  Integer.MAX_VALUE; 废弃
* PARALLELISM_DEFAULT=-1 ; 默认并行度, 可以被用来重置并行度到默认的状态
* PARALLELISM_UNKNOWN =-2 
* DEFAULT_RESTART_DELAY = 10000L
* ExecutionMode executionMode = ExecutionMode.PIPELINED;  定义什么数据发生变化 batch or pipelined
* boolean useClosureCleaner = true;
* int parallelism = PARALLELISM_DEFAULT;
####程序宽最大并行度，用于没有指定最大并行度的操作符。最大并行性指定动态缩放的上限和用于分区状态的键组的数量。
* int maxParallelism = -1;
* boolean forceKryo = false;
####标志，指示是否支持泛型类型(通过Kryo)
* boolean disableGenericTypes = false;
* boolean objectReuse = false;
* boolean autoTypeRegistrationEnabled = true;
* boolean forceAvro = false;
* CodeAnalysisMode codeAnalysisMode = CodeAnalysisMode.DISABLE;

#### 是否打印程序的执行进度
* boolean printProgressDuringExecution = true;
* long autoWatermarkInterval = 0;

#### 以毫秒为单位的间隔，用于将延迟跟踪标记从源发送到接收器。
* long latencyTrackingInterval = MetricOptions.LATENCY_INTERVAL.defaultValue();
* boolean isLatencyTrackingConfigured = false;


 
 ### ArchivedExecutionConfig
    class ArchivedExecutionConfig implements Serializable
    在存档作业时创建的可序列化类。




