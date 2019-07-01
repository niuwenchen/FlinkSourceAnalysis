package com.jackniu.flink.api.common.typeinfo;

import com.jackniu.flink.api.common.functions.InvalidTypesException;
import com.jackniu.flink.api.java.typeutils.TypeExtractor;
import com.jackniu.flink.util.FlinkRuntimeException;

/**
 * Created by JackNiu on 2019/6/25.
 */
public abstract class TypeHint<T> {
    /** The type information described by the hint. */
    private final TypeInformation<T> typeInfo;

    /**
     * Creates a hint for the generic type in the class signature.
     */
    public TypeHint() {
        try {
            this.typeInfo = TypeExtractor.createTypeInfo(
                    this, TypeHint.class, getClass(), 0);
        }
        catch (InvalidTypesException e) {
            throw new FlinkRuntimeException("The TypeHint is using a generic variable." +
                    "This is not supported, generic types must be fully specified for the TypeHint.");
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the type information described by this TypeHint.
     * @return The type information described by this TypeHint.
     */
    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return typeInfo.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
                obj instanceof TypeHint && this.typeInfo.equals(((TypeHint<?>) obj).typeInfo);
    }

    @Override
    public String toString() {
        return "TypeHint: " + typeInfo;
    }
}
