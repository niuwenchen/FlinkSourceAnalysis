package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * Created by JackNiu on 2019/6/20.
 */
public  final class CompatibilityResult<T> {
    /** Whether or not migration is required. */
    private final boolean requiresMigration;

    private final TypeDeserializer<T> convertDeserializer;
    public static <T> CompatibilityResult<T> compatible() {
        return new CompatibilityResult<>(false, null);
    }

    public static <T> CompatibilityResult<T> requiresMigration(@Nonnull TypeDeserializer<T> convertDeserializer) {
        Preconditions.checkNotNull(convertDeserializer, "Convert deserializer cannot be null.");

        return new CompatibilityResult<>(true, convertDeserializer);
    }
    public static <T> CompatibilityResult<T> requiresMigration(@Nonnull TypeSerializer<T> convertSerializer) {
        Preconditions.checkNotNull(convertSerializer, "Convert serializer cannot be null.");

        return new CompatibilityResult<>(true, new TypeDeserializerAdapter<>(convertSerializer));
    }

    public static <T> CompatibilityResult<T> requiresMigration() {
        return new CompatibilityResult<>(true, null);
    }

    private CompatibilityResult(boolean requiresMigration, TypeDeserializer<T> convertDeserializer) {
        this.requiresMigration = requiresMigration;
        this.convertDeserializer = convertDeserializer;
    }

    public TypeDeserializer<T> getConvertDeserializer() {
        return convertDeserializer;
    }

    public boolean isRequiresMigration() {
        return requiresMigration;
    }


}
