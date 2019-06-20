package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class TypeSerializerSchemaCompatibility<T> {
    enum Type {
        /** This indicates that the new serializer continued to be used as is. */
        COMPATIBLE_AS_IS,

        /**
         * This indicates that it is possible to use the new serializer after performing a
         * full-scan migration over all state, by reading bytes with the previous serializer
         * and then writing it again with the new serializer, effectively converting the
         * serialization schema to correspond to the new serializer.
         */
        COMPATIBLE_AFTER_MIGRATION,

        /**
         * This indicates that the new serializer is incompatible, even with migration.
         * This normally implies that the deserialized Java class can not be commonly recognized
         * by the previous and new serializer.
         */
        INCOMPATIBLE
    }
    private final Type resultType;

    public static <T> TypeSerializerSchemaCompatibility<T> compatibleAsIs() {
        return new TypeSerializerSchemaCompatibility<>(Type.COMPATIBLE_AS_IS, null);
    }

    public static <T> TypeSerializerSchemaCompatibility<T> compatibleAfterMigration() {
        return new TypeSerializerSchemaCompatibility<>(Type.COMPATIBLE_AFTER_MIGRATION, null);
    }

    public static <T> TypeSerializerSchemaCompatibility<T> incompatible() {
        return new TypeSerializerSchemaCompatibility<>(Type.INCOMPATIBLE, null);
    }

    private TypeSerializerSchemaCompatibility(Type resultType, @Nullable TypeSerializer<T> reconfiguredNewSerializer) {
        this.resultType = Preconditions.checkNotNull(resultType);
    }

    public boolean isCompatibleAsIs() {
        return resultType == Type.COMPATIBLE_AS_IS;
    }

    /**
     * Returns whether or not the type of the compatibility is {@link Type#COMPATIBLE_AFTER_MIGRATION}.
     *
     * @return whether or not the type of the compatibility is {@link Type#COMPATIBLE_AFTER_MIGRATION}.
     */
    public boolean isCompatibleAfterMigration() {
        return resultType == Type.COMPATIBLE_AFTER_MIGRATION;
    }

    /**
     * Returns whether or not the type of the compatibility is {@link Type#INCOMPATIBLE}.
     *
     * @return whether or not the type of the compatibility is {@link Type#INCOMPATIBLE}.
     */
    public boolean isIncompatible() {
        return resultType == Type.INCOMPATIBLE;
    }

}
