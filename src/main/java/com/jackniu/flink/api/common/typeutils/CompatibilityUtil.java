package com.jackniu.flink.api.common.typeutils;

import javax.annotation.Nullable;

/**
 * Created by JackNiu on 2019/6/28.
 */
public class CompatibilityUtil {
    @SuppressWarnings("unchecked")
    public static <T> CompatibilityResult<T> resolveCompatibilityResult(
            @Nullable TypeSerializer<?> precedingSerializer,
            Class<?> dummySerializerClassTag,
            TypeSerializerSnapshot<?> precedingSerializerConfigSnapshot,
            TypeSerializer<T> newSerializer) {

        if (precedingSerializerConfigSnapshot != null
                && !(precedingSerializerConfigSnapshot instanceof BackwardsCompatibleSerializerSnapshot)) {

            CompatibilityResult<T> initialResult = resolveCompatibilityResult(
                    (TypeSerializerSnapshot<T>) precedingSerializerConfigSnapshot,
                    newSerializer);

            if (!initialResult.isRequiresMigration()) {
                return initialResult;
            } else {
                if (precedingSerializer != null && !(precedingSerializer.getClass().equals(dummySerializerClassTag))) {
                    // if the preceding serializer exists and is not a dummy, use
                    // that for converting instead of any provided convert deserializer
                    return CompatibilityResult.requiresMigration((TypeSerializer<T>) precedingSerializer);
                } else {
                    // requires migration (may or may not have a convert deserializer)
                    return initialResult;
                }
            }
        } else {
            // if the configuration snapshot of the preceding serializer cannot be provided,
            // we can only simply assume that the new serializer is compatible
            return CompatibilityResult.compatible();
        }
    }

    public static <T> CompatibilityResult<T> resolveCompatibilityResult(
            TypeSerializerSnapshot<T> precedingSerializerConfigSnapshot,
            TypeSerializer<T> newSerializer) {

        TypeSerializerSchemaCompatibility<T> compatibility =
                precedingSerializerConfigSnapshot.resolveSchemaCompatibility(newSerializer);

        // everything except "compatible" maps to "requires migration".
        // at the entry point of the new-to-old-bridge (in the TypeSerializerConfigSnapshot), we
        // interpret "requiresMigration" as 'incompatible'. That is a precaution because
        // serializers could previously not specify the 'incompatible' case.
        return compatibility.isCompatibleAsIs() ?
                CompatibilityResult.compatible() :
                CompatibilityResult.requiresMigration();
    }
}
