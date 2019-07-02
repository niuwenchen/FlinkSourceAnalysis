package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.java.typeutils.runtime.EitherSerializer;
import com.jackniu.flink.types.Either;

import java.util.HashMap;
import java.util.Map;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/1.
 */
public class EitherTypeInfo<L, R> extends TypeInformation<Either<L, R>> {
    private static final long serialVersionUID = 1L;

    private final TypeInformation<L> leftType;

    private final TypeInformation<R> rightType;

    @PublicEvolving
    public EitherTypeInfo(TypeInformation<L> leftType, TypeInformation<R> rightType) {
        this.leftType = checkNotNull(leftType);
        this.rightType = checkNotNull(rightType);
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

    @SuppressWarnings("unchecked")
    @Override
    @PublicEvolving
    public Class<Either<L, R>> getTypeClass() {
        return (Class<Either<L, R>>) (Class<?>) Either.class;
    }

    @Override
    @PublicEvolving
    public Map<String, TypeInformation<?>> getGenericParameters() {
        Map<String, TypeInformation<?>> m = new HashMap<>();
        m.put("L", this.leftType);
        m.put("R", this.rightType);
        return m;
    }

    @Override
    @PublicEvolving
    public boolean isKeyType() {
        return false;
    }

    @Override
    @PublicEvolving
    public TypeSerializer<Either<L, R>> createSerializer(ExecutionConfig config) {
        return new EitherSerializer<L, R>(leftType.createSerializer(config),
                rightType.createSerializer(config));
    }

    @Override
    public String toString() {
        return "Either <" + leftType.toString() + ", " + rightType.toString() + ">";
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EitherTypeInfo) {
            EitherTypeInfo<L, R> other = (EitherTypeInfo<L, R>) obj;

            return other.canEqual(this) &&
                    leftType.equals(other.leftType) &&
                    rightType.equals(other.rightType);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 17 * leftType.hashCode() + rightType.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof EitherTypeInfo;
    }

    // --------------------------------------------------------------------------------------------

    public TypeInformation<L> getLeftType() {
        return leftType;
    }

    public TypeInformation<R> getRightType() {
        return rightType;
    }
}
