package com.jackniu.flink.api.common.typeinfo;

import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;
import java.util.HashSet;

import static com.jackniu.flink.util.Preconditions.checkArgument;

/**
 * Created by JackNiu on 2019/6/20.
 */
public abstract class NumericTypeInfo<T> extends BasicTypeInfo<T>  {
    private static final long serialVersionUID = -5937777910658986986L;

    private static final HashSet<Class<?>> numericalTypes = new HashSet<>(
            Arrays.asList(
                    Integer.class,
                    Long.class,
                    Double.class,
                    Byte.class,
                    Short.class,
                    Float.class,
                    Character.class));

    protected NumericTypeInfo(Class<T> clazz, Class<?>[] possibleCastTargetTypes, TypeSerializer<T> serializer, Class<? extends
            TypeComparator<T>> comparatorClass) {
        super(clazz, possibleCastTargetTypes, serializer, comparatorClass);

        checkArgument(numericalTypes.contains(clazz),
                "The given class %s is not a numerical type", clazz.getSimpleName());
    }
}

