package com.jackniu.flink.annotations.docs;

import com.jackniu.flink.annotations.Internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by JackNiu on 2019/6/19.
 */
public final class Documentation {
    public Documentation(){

    }

    @Target({ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface ExcludeFromDocumentation {
        String value() default "";
    }

    @Target({ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface CommonOption {
        int POSITION_MEMORY = 10;
        int POSITION_PARALLELISM_SLOTS = 20;
        int POSITION_FAULT_TOLERANCE = 30;
        int POSITION_HIGH_AVAILABILITY = 40;
        int POSITION_SECURITY = 50;

        int position() default 2147483647;
    }

    @Target({ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface OverrideDefault {
        String value();
    }
}
