package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.api.common.functions.InvalidTypesException;
import com.jackniu.flink.api.common.typeinfo.TypeInfoFactory;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.types.Either;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by JackNiu on 2019/7/1.
 */
public class EitherTypeInfoFactory<L, R> extends TypeInfoFactory<Either<L, R>> {
    @Override
    public TypeInformation<Either<L, R>> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        TypeInformation<?> leftType = genericParameters.get("L");
        TypeInformation<?> rightType = genericParameters.get("R");

        if (leftType == null) {
            throw new InvalidTypesException("Type extraction is not possible on Either" +
                    " type as it does not contain information about the 'left' type.");
        }

        if (rightType == null) {
            throw new InvalidTypesException("Type extraction is not possible on Either" +
                    " type as it does not contain information about the 'right' type.");
        }

        return new EitherTypeInfo(leftType, rightType);
    }
}
