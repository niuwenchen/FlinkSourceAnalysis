package com.jackniu.flink.core.io;

/**
 * Created by JackNiu on 2019/6/19.
 */

/**
 * This interface is implemented by classes that provide a version number. Versions numbers can be used to differentiate
 * between evolving classes.
 */

public interface Versioned {
    int getVersion();
}
