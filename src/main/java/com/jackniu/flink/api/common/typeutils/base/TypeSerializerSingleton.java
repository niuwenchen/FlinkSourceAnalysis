package com.jackniu.flink.api.common.typeutils.base;

import com.jackniu.flink.api.common.typeutils.*;

/**
 * Created by JackNiu on 2019/6/20.
 */
public abstract class TypeSerializerSingleton<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 8766687317209282373L;

    @Override
    public TypeSerializerSingleton<T> duplicate() {
        return this;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TypeSerializerSingleton) {
            TypeSerializerSingleton<?> other = (TypeSerializerSingleton<?>) obj;

            return other.canEqual(this);
        } else {
            return false;
        }
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        // type serializer singletons should always be parameter-less
        return new ParameterlessTypeSerializerConfig<>(getSerializationFormatIdentifier());
    }

    @Override
    public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
        if (configSnapshot instanceof ParameterlessTypeSerializerConfig
                && isCompatibleSerializationFormatIdentifier(
                ((ParameterlessTypeSerializerConfig<?>) configSnapshot).getSerializationFormatIdentifier())) {

            return CompatibilityResult.compatible();
        } else {
            return CompatibilityResult.requiresMigration();
        }
    }

    /**
     * Subclasses can override this if they know that they are also compatible with identifiers of other formats.
     */
    protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
        return identifier.equals(getClass().getName()) ||
                identifier.equals(getClass().getCanonicalName());
    }

    private String getSerializationFormatIdentifier() {
        return getClass().getName();
    }
}
