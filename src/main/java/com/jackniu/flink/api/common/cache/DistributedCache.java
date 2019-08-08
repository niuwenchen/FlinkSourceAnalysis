package com.jackniu.flink.api.common.cache;

import com.jackniu.flink.configuration.Configuration;
import com.jackniu.flink.core.fs.Path;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class DistributedCache {
    public static class DistributedCacheEntry implements Serializable {

        public String filePath;
        public Boolean isExecutable;
        public boolean isZipped;

        public byte[] blobKey;

        /** Client-side constructor used by the API for initial registration. */
        public DistributedCacheEntry(String filePath, Boolean isExecutable) {
            this(filePath, isExecutable, null);
        }

        /** Client-side constructor used during job-submission for zipped directory. */
        public DistributedCacheEntry(String filePath, boolean isExecutable, boolean isZipped) {
            this(filePath, isExecutable, null, isZipped);
        }

        /** Server-side constructor used during job-submission for zipped directories. */
        public DistributedCacheEntry(String filePath, Boolean isExecutable, byte[] blobKey, boolean isZipped) {
            this.filePath = filePath;
            this.isExecutable = isExecutable;
            this.blobKey = blobKey;
            this.isZipped = isZipped;
        }

        /** Server-side constructor used during job-submission for files. */
        public DistributedCacheEntry(String filePath, Boolean isExecutable, byte[] blobKey){
            this(filePath, isExecutable, blobKey, false);
        }

        @Override
        public String toString() {
            return "DistributedCacheEntry{" +
                    "filePath='" + filePath + '\'' +
                    ", isExecutable=" + isExecutable +
                    ", isZipped=" + isZipped +
                    ", blobKey=" + Arrays.toString(blobKey) +
                    '}';
        }
    }

    // ------------------------------------------------------------------------

    private final Map<String, Future<Path>> cacheCopyTasks;

    public DistributedCache(Map<String, Future<Path>> cacheCopyTasks) {
        this.cacheCopyTasks = cacheCopyTasks;
    }

    // ------------------------------------------------------------------------

    public File getFile(String name) {
        if (name == null) {
            throw new NullPointerException("name must not be null");
        }

        Future<Path> future = cacheCopyTasks.get(name);
        if (future == null) {
            throw new IllegalArgumentException("File with name '" + name + "' is not available." +
                    " Did you forget to register the file?");
        }

        try {
            final Path path = future.get();
            URI tmp = path.makeQualified(path.getFileSystem()).toUri();
            return new File(tmp);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("An error occurred while copying the file.", e.getCause());
        }
        catch (Exception e) {
            throw new RuntimeException("Error while getting the file registered under '" + name +
                    "' from the distributed cache", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities to read/write cache files from/to the configuration
    // ------------------------------------------------------------------------

    public static void writeFileInfoToConfig(String name, DistributedCacheEntry e, Configuration conf) {
        int num = conf.getInteger(CACHE_FILE_NUM, 0) + 1;
        conf.setInteger(CACHE_FILE_NUM, num);
        conf.setString(CACHE_FILE_NAME + num, name);
        conf.setString(CACHE_FILE_PATH + num, e.filePath);
        conf.setBoolean(CACHE_FILE_EXE + num, e.isExecutable || new File(e.filePath).canExecute());
        conf.setBoolean(CACHE_FILE_DIR + num, e.isZipped || new File(e.filePath).isDirectory());
        if (e.blobKey != null) {
            conf.setBytes(CACHE_FILE_BLOB_KEY + num, e.blobKey);
        }
    }

    public static Set<Map.Entry<String, DistributedCacheEntry>> readFileInfoFromConfig(Configuration conf) {
        int num = conf.getInteger(CACHE_FILE_NUM, 0);
        if (num == 0) {
            return Collections.emptySet();
        }

        Map<String, DistributedCacheEntry> cacheFiles = new HashMap<String, DistributedCacheEntry>();
        for (int i = 1; i <= num; i++) {
            String name = conf.getString(CACHE_FILE_NAME + i, null);
            String filePath = conf.getString(CACHE_FILE_PATH + i, null);
            boolean isExecutable = conf.getBoolean(CACHE_FILE_EXE + i, false);
            boolean isDirectory = conf.getBoolean(CACHE_FILE_DIR + i, false);

            byte[] blobKey = conf.getBytes(CACHE_FILE_BLOB_KEY + i, null);
            cacheFiles.put(name, new DistributedCacheEntry(filePath, isExecutable, blobKey, isDirectory));
        }
        return cacheFiles.entrySet();
    }

    private static final String CACHE_FILE_NUM = "DISTRIBUTED_CACHE_FILE_NUM";

    private static final String CACHE_FILE_NAME = "DISTRIBUTED_CACHE_FILE_NAME_";

    private static final String CACHE_FILE_PATH = "DISTRIBUTED_CACHE_FILE_PATH_";

    private static final String CACHE_FILE_EXE = "DISTRIBUTED_CACHE_FILE_EXE_";

    private static final String CACHE_FILE_DIR = "DISTRIBUTED_CACHE_FILE_DIR_";

    private static final String CACHE_FILE_BLOB_KEY = "DISTRIBUTED_CACHE_FILE_BLOB_KEY_";
}
