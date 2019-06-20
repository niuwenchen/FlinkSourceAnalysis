package com.jackniu.flink.types;

import com.jackniu.flink.annotations.Public;
import com.jackniu.flink.core.io.IOReadableWritable;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/19.
 */
@Public
public interface Value extends IOReadableWritable, Serializable {
}
