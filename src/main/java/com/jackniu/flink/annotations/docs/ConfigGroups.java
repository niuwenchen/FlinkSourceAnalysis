package com.jackniu.flink.annotations.docs;

import com.jackniu.flink.annotations.Internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by JackNiu on 2019/6/19.
 */

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Internal

public @interface ConfigGroups {
    ConfigGroup[] groups() default {};

}
