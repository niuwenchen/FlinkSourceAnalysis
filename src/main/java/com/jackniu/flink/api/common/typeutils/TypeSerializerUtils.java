package com.jackniu.flink.api.common.typeutils;

import java.util.Arrays;

/**
 * Created by JackNiu on 2019/6/27.
 */
public final class TypeSerializerUtils {
    public static TypeSerializerSnapshot<?>[] snapshotBackwardsCompatible(
            TypeSerializer<?>... originatingSerializers) {

        return Arrays.stream(originatingSerializers)
                .map((s) -> configureForBackwardsCompatibility(s.snapshotConfiguration(), s))
                .toArray(TypeSerializerSnapshot[]::new);
    }
    @SuppressWarnings({"unchecked", "deprecation"})
    private static <T> TypeSerializerSnapshot<T> configureForBackwardsCompatibility(
            TypeSerializerSnapshot<?> snapshot,
            TypeSerializer<?> serializer) {

        TypeSerializerSnapshot<T> typedSnapshot = (TypeSerializerSnapshot<T>) snapshot;
        TypeSerializer<T> typedSerializer = (TypeSerializer<T>) serializer;

        if (snapshot instanceof TypeSerializerConfigSnapshot) {
            ((TypeSerializerConfigSnapshot<T>) typedSnapshot).setPriorSerializer(typedSerializer);
        }

        return typedSnapshot;
    }

    // ------------------------------------------------------------------------

    /** This class is not meanto to be instantiated. */
    private TypeSerializerUtils() {}

}
