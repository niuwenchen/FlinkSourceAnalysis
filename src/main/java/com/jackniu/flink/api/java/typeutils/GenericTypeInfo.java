package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeinfo.AtomicType;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.java.typeutils.runtime.GenericTypeComparator;
import com.jackniu.flink.api.java.typeutils.runtime.kyro.KryoSerializer;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/6/26.
 */
public class GenericTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {
    private static final long serialVersionUID = -7959114120287706504L;

    private final Class<T> typeClass;

    @PublicEvolving
    public GenericTypeInfo(Class<T> typeClass) {
        this.typeClass = checkNotNull(typeClass);
    }

    @Override
    @PublicEvolving
    public boolean isBasicType() {
        return false;
    }

    @Override
    @PublicEvolving
    public boolean isTupleType() {
        return false;
    }

    @Override
    @PublicEvolving
    public int getArity() {
        return 1;
    }

    @Override
    @PublicEvolving
    public int getTotalFields() {
        return 1;
    }

    @Override
    @PublicEvolving
    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    @PublicEvolving
    public boolean isKeyType() {
        return Comparable.class.isAssignableFrom(typeClass);
    }

    @Override
    @PublicEvolving
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        if (config.hasGenericTypesDisabled()) {
            throw new UnsupportedOperationException(
                    "Generic types have been disabled in the ExecutionConfig and type " + this.typeClass.getName() +
                            " is treated as a generic type.");
        }

        return new KryoSerializer<T>(this.typeClass, config);
    }

    @SuppressWarnings("unchecked")
    @Override
    @PublicEvolving
    public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
        if (isKeyType()) {
            @SuppressWarnings("rawtypes")
            GenericTypeComparator comparator = new GenericTypeComparator(sortOrderAscending, createSerializer(executionConfig), this.typeClass);
            return (TypeComparator<T>) comparator;
        }

        throw new UnsupportedOperationException("Types that do not implement java.lang.Comparable cannot be used as keys.");
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof GenericTypeInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GenericTypeInfo) {
            @SuppressWarnings("unchecked")
            GenericTypeInfo<T> genericTypeInfo = (GenericTypeInfo<T>) obj;

            return typeClass == genericTypeInfo.typeClass;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "GenericType<" + typeClass.getCanonicalName() + ">";
    }
}
