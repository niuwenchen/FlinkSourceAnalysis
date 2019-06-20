package com.jackniu.flink.configuration;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.annotations.docs.ConfigGroup;
import com.jackniu.flink.annotations.docs.ConfigGroups;
import com.jackniu.flink.annotations.docs.Documentation;
import com.jackniu.flink.configuration.description.Description;

import static com.jackniu.flink.configuration.ConfigOptions.key;


/**
 * Created by JackNiu on 2019/6/19.
 */

@PublicEvolving
@ConfigGroups(groups = {
        @ConfigGroup(name = "Environment", keyPrefix = "env"),
        @ConfigGroup(name = "FileSystem", keyPrefix = "fs")
})

public class CoreOptions {


    /**
     * Defines the class resolution strategy when loading classes from user code,
     * meaning whether to first check the user code jar ({@code "child-first"}) or
     * the application classpath ({@code "parent-first"})
     *
     * <p>The default settings indicate to load classes first from the user code jar,
     * which means that user code jars can include and load different dependencies than
     * Flink uses (transitively).
     *
     * <p>Exceptions to the rules are defined via {@link #ALWAYS_PARENT_FIRST_LOADER_PATTERNS}.
     */


    public static final ConfigOption<String> CLASSLOADER_RESOLVE_ORDER =
            key("classloader.resolve-order")
            .defaultValue("child-first")
            .withDescription("Defines the class resolution strategy when loading classes from user code, meaning whether to" +
                    " first check the user code jar (\"child-first\") or the application classpath (\"parent-first\")." +
                    " The default settings indicate to load classes first from the user code jar, which means that user code" +
                    " jars can include and load different dependencies than Flink uses (transitively).");



    public static final ConfigOption<String> ALWAYS_PARENT_FIRST_LOADER_PATTERNS =
            key("classloader.parent-first-patterns.default")
            .defaultValue("java.;scala.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback")
            .withDeprecatedKeys("classloader.parent-first-patterns")
            .withDescription("A (semicolon-separated) list of patterns that specifies which classes should always be" +
                    " resolved through the parent ClassLoader first. A pattern is a simple prefix that is checked against" +
                    " the fully qualified class name. This setting should generally not be modified. To add another pattern we" +
                    " recommend to use \"classloader.parent-first-patterns.additional\" instead.");

    public static final ConfigOption<String> ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL =
            key("classloader.parent-first-patterns.additional")
            .defaultValue("")
            .withDescription("A (semicolon-separated) list of patterns that specifies which classes should always be" +
                    " resolved through the parent ClassLoader first. A pattern is a simple prefix that is checked against" +
                    " the fully qualified class name. These patterns are appended to \"" + ALWAYS_PARENT_FIRST_LOADER_PATTERNS.key() + "\".");

    public static String[] getParentFirstLoaderPatterns(Configuration config) {
        String base = config.getString(ALWAYS_PARENT_FIRST_LOADER_PATTERNS);
        String append = config.getString(ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);

        String[] basePatterns = base.isEmpty()
                ? new String[0]
                : base.split(";");

        if (append.isEmpty()) {
            return basePatterns;
        } else {
            String[] appendPatterns = append.split(";");

            String[] joinedPatterns = new String[basePatterns.length + appendPatterns.length];
            System.arraycopy(basePatterns, 0, joinedPatterns, 0, basePatterns.length);
            System.arraycopy(appendPatterns, 0, joinedPatterns, basePatterns.length, appendPatterns.length);
            return joinedPatterns;
        }
    }

    // ------------------------------------------------------------------------
    //  process parameters
    // ------------------------------------------------------------------------

    public static final ConfigOption<String> FLINK_JVM_OPTIONS =
            key("env.java.opts")
            .defaultValue("")
            .withDescription(Description.builder().text("Java options to start the JVM of all Flink processes with.").build());

    public static final ConfigOption<String> FLINK_JM_JVM_OPTIONS =
            key("env.java.opts.jobmanager")
            .defaultValue("")
            .withDescription(Description.builder().text("Java options to start the JVM of the JobManager with.").build());

    public static final ConfigOption<String> FLINK_TM_JVM_OPTIONS =
            key("env.java.opts.taskmanager")
            .defaultValue("")
            .withDescription(Description.builder().text("Java options to start the JVM of the TaskManager with.").build());

    public static final ConfigOption<String> FLINK_HS_JVM_OPTIONS =
            key("env.java.opts.historyserver")
            .defaultValue("")
            .withDescription(Description.builder().text("Java options to start the JVM of the HistoryServer with.").build());

    /**
     * This options is here only for documentation generation, it is only
     * evaluated in the shell scripts.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<String> FLINK_LOG_DIR =
            key("env.log.dir")
            .noDefaultValue()
            .withDescription("Defines the directory where the Flink logs are saved. It has to be an absolute path." +
                    " (Defaults to the log directory under Flink’s home)");

    /**
     * This options is here only for documentation generation, it is only
     * evaluated in the shell scripts.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<Integer> FLINK_LOG_MAX =
            key("env.log.max")
            .defaultValue(5)
            .withDescription("The maximum number of old log files to keep.");

    /**
     * This options is here only for documentation generation, it is only
     * evaluated in the shell scripts.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<String> FLINK_SSH_OPTIONS =
            key("env.ssh.opts")
            .noDefaultValue()
            .withDescription("Additional command line options passed to SSH clients when starting or stopping JobManager," +
                    " TaskManager, and Zookeeper services (start-cluster.sh, stop-cluster.sh, start-zookeeper-quorum.sh," +
                    " stop-zookeeper-quorum.sh).");

    /**
     * This options is here only for documentation generation, it is only
     * evaluated in the shell scripts.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<String> FLINK_HADOOP_CONF_DIR =
            key("env.hadoop.conf.dir")
            .noDefaultValue()
            .withDescription("Path to hadoop configuration directory. It is required to read HDFS and/or YARN" +
                    " configuration. You can also set it via environment variable.");

    /**
     * This options is here only for documentation generation, it is only
     * evaluated in the shell scripts.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<String> FLINK_YARN_CONF_DIR =
            key("env.yarn.conf.dir")
            .noDefaultValue()
            .withDescription("Path to yarn configuration directory. It is required to run flink on YARN. You can also" +
                    " set it via environment variable.");

    // ------------------------------------------------------------------------
    //  generic io
    // ------------------------------------------------------------------------

    /**
     * The config parameter defining the directories for temporary files, separated by
     * ",", "|", or the system's {@link java.io.File#pathSeparator}.
     */
    @Documentation.OverrideDefault("'LOCAL_DIRS' on Yarn. '_FLINK_TMP_DIR' on Mesos. System.getProperty(\"java.io.tmpdir\") in standalone.")
    public static final ConfigOption<String> TMP_DIRS =
            key("io.tmp.dirs")
                    .defaultValue(System.getProperty("java.io.tmpdir"))
                    .withDeprecatedKeys("taskmanager.tmp.dirs");

    // ------------------------------------------------------------------------
    //  program
    // ------------------------------------------------------------------------

    @Documentation.CommonOption(position = Documentation.CommonOption.POSITION_PARALLELISM_SLOTS)
    public static final ConfigOption<Integer> DEFAULT_PARALLELISM =
            key("parallelism.default")
            .defaultValue(1);

    // ------------------------------------------------------------------------
    //  file systems
    // ------------------------------------------------------------------------

    /**
     * The default filesystem scheme, used for paths that do not declare a scheme explicitly.
     */
    public static final ConfigOption<String> DEFAULT_FILESYSTEM_SCHEME =
            key("fs.default-scheme")
            .noDefaultValue()
            .withDescription("The default filesystem scheme, used for paths that do not declare a scheme explicitly." +
                    " May contain an authority, e.g. host:port in case of a HDFS NameNode.");

    /**
     * Specifies whether file output writers should overwrite existing files by default.
     */
    public static final ConfigOption<Boolean> FILESYTEM_DEFAULT_OVERRIDE =
            key("fs.overwrite-files")
                    .defaultValue(false)
                    .withDescription("Specifies whether file output writers should overwrite existing files by default. Set to" +
                            " \"true\" to overwrite by default,\"false\" otherwise.");

    /**
     * Specifies whether the file systems should always create a directory for the output, even with a parallelism of one.
     */
    public static final ConfigOption<Boolean> FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY =
            key("fs.output.always-create-directory")
                    .defaultValue(false)
                    .withDescription("File writers running with a parallelism larger than one create a directory for the output" +
                            " file path and put the different result files (one per parallel writer task) into that directory." +
                            " If this option is set to \"true\", writers with a parallelism of 1 will also create a" +
                            " directory and place a single result file into it. If the option is set to \"false\"," +
                            " the writer will directly create the file directly at the output path, without creating a containing" +
                            " directory.");

    /**
     * The total number of input plus output connections that a file system for the given scheme may open.
     * Unlimited be default.
     */
    public static ConfigOption<Integer> fileSystemConnectionLimit(String scheme) {
        return key("fs." + scheme + ".limit.total").defaultValue(-1);
    }

    /**
     * The total number of input connections that a file system for the given scheme may open.
     * Unlimited be default.
     */
    public static ConfigOption<Integer> fileSystemConnectionLimitIn(String scheme) {
        return key("fs." + scheme + ".limit.input").defaultValue(-1);
    }

    /**
     * The total number of output connections that a file system for the given scheme may open.
     * Unlimited be default.
     */
    public static ConfigOption<Integer> fileSystemConnectionLimitOut(String scheme) {
        return key("fs." + scheme + ".limit.output").defaultValue(-1);
    }

    /**
     * If any connection limit is configured, this option can be optionally set to define after
     * which time (in milliseconds) stream opening fails with a timeout exception, if no stream
     * connection becomes available. Unlimited timeout be default.
     */
    public static ConfigOption<Long> fileSystemConnectionLimitTimeout(String scheme) {
        return key("fs." + scheme + ".limit.timeout").defaultValue(0L);
    }

    /**
     * If any connection limit is configured, this option can be optionally set to define after
     * which time (in milliseconds) inactive streams are reclaimed. This option can help to prevent
     * that inactive streams make up the full pool of limited connections, and no further connections
     * can be established. Unlimited timeout be default.
     */
    public static ConfigOption<Long> fileSystemConnectionLimitStreamInactivityTimeout(String scheme) {
        return key("fs." + scheme + ".limit.stream-timeout").defaultValue(0L);
    }

}
