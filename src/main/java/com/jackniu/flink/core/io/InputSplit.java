package com.jackniu.flink.core.io;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface InputSplit extends Serializable {
    int getSplitNumber();
}
