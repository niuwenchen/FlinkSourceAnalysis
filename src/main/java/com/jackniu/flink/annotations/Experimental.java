package com.jackniu.flink.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Created by JackNiu on 2019/6/19.
 */

@Documented
@Target({ElementType.TYPE})
@Public

public @interface Experimental {
}
