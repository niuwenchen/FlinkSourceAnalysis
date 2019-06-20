package com.jackniu.flink.annotations.docs;

import com.jackniu.flink.annotations.Internal;

import java.lang.annotation.Target;

/**
 * Created by JackNiu on 2019/6/19.
 */
@Target({})
@Internal
public @interface ConfigGroup {
    String name();

    String keyPrefix();
}
