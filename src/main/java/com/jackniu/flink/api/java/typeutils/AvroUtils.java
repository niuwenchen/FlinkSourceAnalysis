package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.java.typeutils.runtime.KryoRegistration;
import com.jackniu.flink.api.java.typeutils.runtime.kyro.Serializers;
import static com.jackniu.flink.api.java.typeutils.TypeExtractionUtils.hasSuperclass;
import java.util.LinkedHashMap;

/**
 * Created by JackNiu on 2019/6/27.
 */
public abstract class AvroUtils {
    private static final String AVRO_KRYO_UTILS = "org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils";

    public static AvroUtils getAvroUtils() {
        // try and load the special AvroUtils from the flink-avro package
        try {
            Class<?> clazz = Class.forName(AVRO_KRYO_UTILS, false, Thread.currentThread().getContextClassLoader());
            return clazz.asSubclass(AvroUtils.class).getConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            // cannot find the utils, return the default implementation
            return new DefaultAvroUtils();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate " + AVRO_KRYO_UTILS + ".", e);
        }
    }

    public abstract void addAvroSerializersIfRequired(ExecutionConfig reg, Class<?> type);
    public abstract void addAvroGenericDataArrayRegistration(
            LinkedHashMap<String,
                    KryoRegistration> kryoRegistrations);

    /**
     * Creates an {@code AvroSerializer} if flink-avro is present, otherwise throws an exception.
     */
    public abstract <T> TypeSerializer<T> createAvroSerializer(Class<T> type);

    /**
     * Creates an {@code AvroTypeInfo} if flink-avro is present, otherwise throws an exception.
     */
    public abstract <T> TypeInformation<T> createAvroTypeInfo(Class<T> type);

    /**
     * A default implementation of the AvroUtils used in the absence of Avro.
     */
    private static class DefaultAvroUtils extends AvroUtils {

        private static final String AVRO_SPECIFIC_RECORD_BASE = "org.apache.avro.specific.SpecificRecordBase";

        private static final String AVRO_GENERIC_RECORD = "org.apache.avro.generic.GenericData$Record";

        private static final String AVRO_GENERIC_DATA_ARRAY = "org.apache.avro.generic.GenericData$Array";

        @Override
        public void addAvroSerializersIfRequired(ExecutionConfig reg, Class<?> type) {
            if (hasSuperclass(type, AVRO_SPECIFIC_RECORD_BASE) ||
                    hasSuperclass(type, AVRO_GENERIC_RECORD)) {

                throw new RuntimeException("Could not load class '" + AVRO_KRYO_UTILS + "'. " +
                        "You may be missing the 'flink-avro' dependency.");
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public void addAvroGenericDataArrayRegistration(LinkedHashMap<String, KryoRegistration> kryoRegistrations) {
            kryoRegistrations.put(AVRO_GENERIC_DATA_ARRAY,
                    new KryoRegistration(Serializers.DummyAvroRegisteredClass.class, (Class) Serializers.DummyAvroKryoSerializerClass.class));
        }

        @Override
        public <T> TypeSerializer<T> createAvroSerializer(Class<T> type) {
            throw new RuntimeException("Could not load the AvroSerializer class. " +
                    "You may be missing the 'flink-avro' dependency.");
        }

        @Override
        public <T> TypeInformation<T> createAvroTypeInfo(Class<T> type) {
            throw new RuntimeException("Could not load the AvroTypeInfo class. " +
                    "You may be missing the 'flink-avro' dependency.");
        }
    }

}
