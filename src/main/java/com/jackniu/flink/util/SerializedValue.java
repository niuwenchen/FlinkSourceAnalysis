package com.jackniu.flink.util;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class SerializedValue<T> implements java.io.Serializable {

    private static final long serialVersionUID = -3564011643393683761L;

    /** The serialized data. */
    private final byte[] serializedData;

    private SerializedValue(byte[] serializedData) {
        Preconditions.checkNotNull(serializedData, "Serialized data");
        this.serializedData = serializedData;
    }

    public SerializedValue(T value) throws IOException {
        this.serializedData = value == null ? null : InstantiationUtil.serializeObject(value);
    }

    @SuppressWarnings("unchecked")
    public T deserializeValue(ClassLoader loader) throws IOException, ClassNotFoundException {
        Preconditions.checkNotNull(loader, "No classloader has been passed");
        return serializedData == null ? null : (T) InstantiationUtil.deserializeObject(serializedData, loader);
    }

    /**
     * Returns the serialized value or <code>null</code> if no value is set.
     *
     * @return Serialized data.
     */
    public byte[] getByteArray() {
        return serializedData;
    }

    public static <T> SerializedValue<T> fromBytes(byte[] serializedData) {
        return new SerializedValue<>(serializedData);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return serializedData == null ? 0 : Arrays.hashCode(serializedData);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SerializedValue) {
            SerializedValue<?> other = (SerializedValue<?>) obj;
            return this.serializedData == null ? other.serializedData == null :
                    (other.serializedData != null && Arrays.equals(this.serializedData, other.serializedData));
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "SerializedValue";
    }
}

