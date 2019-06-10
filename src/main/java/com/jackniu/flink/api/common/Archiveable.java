package com.jackniu.flink.api.common;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/6.
 */
public interface Archiveable <T extends Serializable> {
    T archive();
}
