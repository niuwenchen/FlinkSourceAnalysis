package com.jackniu.flink.api.common.typeinfo;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by JackNiu on 2019/6/20.
 */
public abstract class TypeInfoFactory<T> {
    public abstract TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters);
}
