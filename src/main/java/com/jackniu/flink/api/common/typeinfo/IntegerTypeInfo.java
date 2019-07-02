package com.jackniu.flink.api.common.typeinfo;

import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;
import java.util.HashSet;

import static com.jackniu.flink.util.Preconditions.checkArgument;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class IntegerTypeInfo<T> extends NumericTypeInfo<T> {
    private static final long serialVersionUID = -8068827354966766955L;

    private static final HashSet<Class<?>> integerTypes = new HashSet<>(
            Arrays.asList(
                    Integer.class,
                    Long.class,
                    Byte.class,
                    Short.class,
                    Character.class));

    protected IntegerTypeInfo(Class<T> clazz, Class<?>[] possibleCastTargetTypes, TypeSerializer<T> serializer, Class<? extends TypeComparator<T>> comparatorClass) {
        super(clazz, possibleCastTargetTypes, serializer, comparatorClass);

        checkArgument(integerTypes.contains(clazz),
                "The given class %s is not a integer type.", clazz.getSimpleName());
    }


}
