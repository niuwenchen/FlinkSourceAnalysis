package com.jackniu.flink.configuration;

import static com.jackniu.flink.configuration.ConfigOptions.key;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class BlobServerOptions {




    /**
     * Cleanup interval of the blob caches at the task managers (in seconds).
     *
     * <p>Whenever a job is not referenced at the cache anymore, we set a TTL and let the periodic
     * cleanup task (executed every CLEANUP_INTERVAL seconds) remove its blob files after this TTL
     * has passed. This means that a blob will be retained at most <tt>2 * CLEANUP_INTERVAL</tt>
     * seconds after not being referenced anymore. Therefore, a recovery still has the chance to use
     * existing files rather than to download them again.
     */
    public static final ConfigOption<Long> CLEANUP_INTERVAL =
            key("blob.service.cleanup.interval")
                    .defaultValue(3_600L) // once per hour
                    .withDeprecatedKeys("library-cache-manager.cleanup.interval")
                    .withDescription("Cleanup interval of the blob caches at the task managers (in seconds).");



}
