package com.jackniu.flink.core.io;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface SimpleVersionedSerializer<E>  extends Versioned{
    /**
     * Gets the version with which this serializer serializes.
     *
     * @return The version of the serialization schema.
     */
    @Override
    int getVersion();

    /**
     * Serializes the given object. The serialization is assumed to correspond to the
     * current serialization version (as returned by {@link #getVersion()}.
     *
     * @param obj The object to serialize.
     * @return The serialized data (bytes).
     *
     * @throws IOException Thrown, if the serialization fails.
     */
    byte[] serialize(E obj) throws IOException;

    /**
     * De-serializes the given data (bytes) which was serialized with the scheme of the
     * indicated version.
     *
     * @param version The version in which the data was serialized
     * @param serialized The serialized data
     * @return The deserialized object
     *
     * @throws IOException Thrown, if the deserialization fails.
     */
    E deserialize(int version, byte[] serialized) throws IOException;
}
