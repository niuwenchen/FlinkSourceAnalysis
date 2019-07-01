package com.jackniu.flink.api.java.typeutils;

/**
 * Created by JackNiu on 2019/6/25.
 */

import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.functions.InvalidTypesException;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;

/**
 * A special type information signifying that the type extraction failed. It contains
 * additional error information.
 */

public class MissingTypeInfo extends TypeInformation<InvalidTypesException> {
    private static final long serialVersionUID = -4212082837126702723L;

    private final String functionName;
    private final InvalidTypesException typeException;


    public MissingTypeInfo(String functionName) {
        this(functionName, new InvalidTypesException("An unknown error occured."));
    }

    public MissingTypeInfo(String functionName, InvalidTypesException typeException) {
        this.functionName = functionName;
        this.typeException = typeException;
    }

    // --------------------------------------------------------------------------------------------

    public String getFunctionName() {
        return functionName;
    }

    public InvalidTypesException getTypeException() {
        return typeException;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean isBasicType() {
        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
    }

    @Override
    public boolean isTupleType() {
        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
    }

    @Override
    public int getArity() {
        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
    }

    @Override
    public Class<InvalidTypesException> getTypeClass() {
        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
    }

    @Override
    public boolean isKeyType() {
        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
    }

//    @Override
//    public TypeSerializer<InvalidTypesException> createSerializer(ExecutionConfig executionConfig) {
//        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
//    }


    @Override
    public TypeSerializer<InvalidTypesException> createSerializer(ExecutionConfig config) {
        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "<" + functionName + ", " + typeException.getMessage() + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MissingTypeInfo) {
            MissingTypeInfo missingTypeInfo = (MissingTypeInfo) obj;

            return missingTypeInfo.canEqual(this) &&
                    functionName.equals(missingTypeInfo.functionName) &&
                    typeException.equals(missingTypeInfo.typeException);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * functionName.hashCode() + typeException.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof MissingTypeInfo;
    }

    @Override
    public int getTotalFields() {
        throw new UnsupportedOperationException("The missing type information cannot be used as a type information.");
    }
}
