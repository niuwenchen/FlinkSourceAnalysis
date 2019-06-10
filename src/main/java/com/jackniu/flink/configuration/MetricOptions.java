package com.jackniu.flink.configuration;

/**
 * Created by JackNiu on 2019/6/6.
 *  Configuration options for metrics and metric reporters.
 *
 * 可选的记者姓名列表。如果配置成功，将只启动名称与列表中任何名称匹配的记者。否则，将启动在配置中可以找到的所有记者。
 * examples
 *  metrics.reporters = foo,bar
 *  metrics.reporter.foo.class = org.apache.flink.metrics.reporter.JMXReporter
 *  metrics.reporter.foo.interval = 10
 *
 *  metrics.reporter.bar.class = org.apache.flink.metrics.graphite.GraphiteReporter
 *  metrics.reporter.bar.port = 1337
 */

import com.jackniu.flink.configuration.description.Description;

import static com.jackniu.flink.configuration.ConfigOptions.key;
import static com.jackniu.flink.configuration.description.TextElement.text;

public class MetricOptions {
    public static final ConfigOption<String> REPORTERS_LIST= key("metrics.reporters").noDefaultValue();
    public static final ConfigOption<String> REPORTER_CLASS =key("metrics.reporter.<name>.class").noDefaultValue()
            .withDescription("The reporter class to use for the reporter named <name>.");
    public static final ConfigOption<String> REPORTER_INTERVAL =
            key("metrics.reporter.<name>.interval")
                    .noDefaultValue()
                    .withDescription("The reporter interval to use for the reporter named <name>.");

    public static final ConfigOption<String> REPORTER_CONFIG_PARAMETER =
            key("metrics.reporter.<name>.<parameter>")
                    .noDefaultValue()
                    .withDescription("Configures the parameter <parameter> for the reporter named <name>.");

    //用于组装度量标识符的分隔符
    public static final ConfigOption<String> SCOPE_DELIMITER =
            key("metrics.scope.delimiter")
                    .defaultValue(".");

    //作用域格式字符串，应用于作业管理器作用域内的所有度量。
    public static final ConfigOption<String> SCOPE_NAMING_JM =
            key("metrics.scope.jm")
                    .defaultValue("<host>.jobmanager")
                    .withDescription("Defines the scope format string that is applied to all metrics scoped to a JobManager.");


    //作用域格式字符串，应用于任务管理器作用域内的所有度量。
    public static final ConfigOption<String> SCOPE_NAMING_TM =
            key("metrics.scope.tm")
                    .defaultValue("<host>.taskmanager.<tm_id>")
                    .withDescription("Defines the scope format string that is applied to all metrics scoped to a TaskManager.");

    public static final ConfigOption<String> SCOPE_NAMING_JM_JOB =
            key("metrics.scope.jm.job")
                    .defaultValue("<host>.jobmanager.<job_name>")
                    .withDescription("Defines the scope format string that is applied to all metrics scoped to a job on a JobManager.");

    public static final ConfigOption<String> SCOPE_NAMING_TM_JOB =
            key("metrics.scope.tm.job")
                    .defaultValue("<host>.taskmanager.<tm_id>.<job_name>")
                    .withDescription("Defines the scope format string that is applied to all metrics scoped to a job on a TaskManager.");

    public static final ConfigOption<String> SCOPE_NAMING_TASK =
            key("metrics.scope.task")
                    .defaultValue("<host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>")
                    .withDescription("Defines the scope format string that is applied to all metrics scoped to a task.");

    public static final ConfigOption<String> SCOPE_NAMING_OPERATOR =
            key("metrics.scope.operator")
                    .defaultValue("<host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>")
                    .withDescription("Defines the scope format string that is applied to all metrics scoped to an operator.");


    public static final ConfigOption<Long> LATENCY_INTERVAL =
            key("metrics.latency.interval")
                    .defaultValue(0L)
                    .withDescription("Defines the interval at which latency tracking marks are emitted from the sources." +
                            " Disables latency tracking if set to 0 or a negative value. Enabling this feature can significantly" +
                            " impact the performance of the cluster.");

    public static final ConfigOption<String> LATENCY_SOURCE_GRANULARITY =
            key("metrics.latency.granularity")
                    .defaultValue("operator")
                    .withDescription(Description.builder()
                            .text("Defines the granularity of latency metrics. Accepted values are:")
                            .list(
                                    text("single - Track latency without differentiating between sources and subtasks."),
                                    text("operator - Track latency while differentiating between sources, but not subtasks."),
                                    text("subtask - Track latency while differentiating between sources and subtasks."))
                            .build());

    public static final ConfigOption<Integer> LATENCY_HISTORY_SIZE =
            key("metrics.latency.history-size")
                    .defaultValue(128)
                    .withDescription("Defines the number of measured latencies to maintain at each operator.");


    //Flink是否应该报告系统资源指标，如机器的CPU、内存或网络使用情况。
    public static final ConfigOption<Boolean> SYSTEM_RESOURCE_METRICS =
            key("metrics.system-resource")
                    .defaultValue(false);

    public static final ConfigOption<Long> SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL =
            key("metrics.system-resource-probing-interval")
                    .defaultValue(5000L);

    public static final ConfigOption<String> QUERY_SERVICE_PORT =
            key("metrics.internal.query-service.port")
                    .defaultValue("0")
                    .withDescription("The port range used for Flink's internal metric query service. Accepts a list of ports " +
                            "(“50100,50101”), ranges(“50100-50200”) or a combination of both. It is recommended to set a range of " +
                            "ports to avoid collisions when multiple Flink components are running on the same machine. Per default " +
                            "Flink will pick a random port.");

    //Flink内部度量查询服务的线程优先级 1 表示最小优先级 10 代表最大优先级
    public static final ConfigOption<Integer> QUERY_SERVICE_THREAD_PRIORITY =
            key("metrics.internal.query-service.thread-priority")
                    .defaultValue(1)
                    .withDescription("The thread priority used for Flink's internal metric query service. The thread is created" +
                            " by Akka's thread pool executor. " +
                            "The range of the priority is from 1 (MIN_PRIORITY) to 10 (MAX_PRIORITY). " +
                            "Warning, increasing this value may bring the main Flink components down.");

    private MetricOptions() {
    }

}
