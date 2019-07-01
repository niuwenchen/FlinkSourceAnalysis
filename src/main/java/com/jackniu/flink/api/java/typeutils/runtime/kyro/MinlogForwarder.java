package com.jackniu.flink.api.java.typeutils.runtime.kyro;

import com.esotericsoftware.minlog.Log;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/6/28.
 */
public class MinlogForwarder extends Log.Logger {
    private final org.slf4j.Logger log;

    MinlogForwarder(org.slf4j.Logger log) {
        this.log = checkNotNull(log);
    }

    @Override
    public void log (int level, String category, String message, Throwable ex) {
        final String logString = "[KRYO " + category + "] " + message;
        switch (level) {
            case Log.LEVEL_ERROR:
                log.error(logString, ex);
                break;
            case Log.LEVEL_WARN:
                log.warn(logString, ex);
                break;
            case Log.LEVEL_INFO:
                log.info(logString, ex);
                break;
            case Log.LEVEL_DEBUG:
                log.debug(logString, ex);
                break;
            case Log.LEVEL_TRACE:
                log.trace(logString, ex);
                break;
        }
    }

}
