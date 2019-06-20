package com.jackniu.flink.api.common.typeinfo;

import com.jackniu.flink.annotations.PublicEvolving;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Created by JackNiu on 2019/6/17.
 */
/**
 * 核心类
 *
 * */
public abstract class TypeInformation<T> implements Serializable{
    private static final long serialVersionUID= -7742311969684489493L;
    /**
     * Checks if this type information represents a basic type.
     * Basic types are defined in {@link BasicTypeInfo} and are primitives, their boxing types,
     * Strings, Date, Void, ...
     *
     * @return True, if this type information describes a basic type, false otherwise.
     */
    public abstract boolean isBasicType();


    @PublicEvolving
    public abstract boolean isTupleType();

    @PublicEvolving
    public  abstract int getArity();

    @PublicEvolving
    public abstract int getTotalFields();

    @PublicEvolving
    public abstract Class<T> getTypeClass();

    @PublicEvolving
    public Map<String, TypeInformation<?>> getGenericParameters() {
        // return an empty map as the default implementation
        return Collections.emptyMap();
    }
    @PublicEvolving
    public abstract boolean isKeyType();
    @PublicEvolving
    public boolean isSortKeyType() {
        return isKeyType();
    }

    @PublicEvolving
    public abstract TypeSerializer<T> createSerializer(ExecutionConfig config);

    @Override
    public abstract String toString();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    /**
     * Returns true if the given object can be equaled with this object. If not, it returns false.
     *
     * @param obj Object which wants to take part in the equality relation
     * @return true if obj can be equaled with this, otherwise false
     */
    public abstract boolean canEqual(Object obj);

    public static <T> TypeInformation<T> of(Class<T> typeClass) {
        try {
            return TypeExtractor.createTypeInfo(typeClass);
        }
        catch (InvalidTypesException e) {
            throw new FlinkRuntimeException(
                    "Cannot extract TypeInformation from Class alone, because generic parameters are missing. " +
                            "Please use TypeInformation.of(TypeHint) instead, or another equivalent method in the API that " +
                            "accepts a TypeHint instead of a Class. " +
                            "For example for a Tuple2<Long, String> pass a 'new TypeHint<Tuple2<Long, String>>(){}'.");
        }
    }

    public static <T> TypeInformation<T> of(TypeHint<T> typeHint) {
        return typeHint.getTypeInfo();
    }

}
