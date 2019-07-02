package com.jackniu.flink.api.common.typeinfo;

import com.jackniu.flink.annotations.Public;

import java.lang.annotation.*;

/**
 * Created by JackNiu on 2019/7/1.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Public
public @interface TypeInfo {
    Class<? extends TypeInfoFactory> value();
}
