package com.jackniu.flink.api.common.typeutils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.java.typeutils.runtime.DataInputViewStream;
import com.jackniu.flink.api.java.typeutils.runtime.DataOutputViewStream;
import com.jackniu.flink.api.java.typeutils.runtime.KryoRegistration;
import com.jackniu.flink.api.java.typeutils.runtime.kyro.KryoSerializer;
import com.jackniu.flink.core.io.IOReadableWritable;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.InstantiationUtil;
import com.jackniu.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by JackNiu on 2019/6/20.
 */
public abstract class KryoRegistrationSerializerConfigSnapshot<T> extends GenericTypeSerializerConfigSnapshot<T>  {

    private static final Logger LOG = LoggerFactory.getLogger(KryoRegistrationSerializerConfigSnapshot.class);


    public KryoRegistrationSerializerConfigSnapshot(){}

    /** Map of class tag to the registration, with ordering. */
    private LinkedHashMap<String, KryoRegistration> kryoRegistrations;
    public KryoRegistrationSerializerConfigSnapshot(Class<T> typeClass,
                                                    LinkedHashMap<String, KryoRegistration> kryoRegistrations){
        super(typeClass);

        this.kryoRegistrations = Preconditions.checkNotNull(kryoRegistrations);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);

        out.writeInt(kryoRegistrations.size());
        for (Map.Entry<String, KryoRegistration> kryoRegistrationEntry : kryoRegistrations.entrySet()) {
            out.writeUTF(kryoRegistrationEntry.getKey());
            new KryoRegistrationSerializationProxy<>(kryoRegistrationEntry.getValue()).write(out);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);

        int numKryoRegistrations = in.readInt();
        kryoRegistrations = new LinkedHashMap<>(numKryoRegistrations);

        KryoRegistrationSerializationProxy proxy;
        for (int i = 0; i < numKryoRegistrations; i++) {
            String classTag = in.readUTF();

            proxy = new KryoRegistrationSerializationProxy(getUserCodeClassLoader());
            proxy.read(in);

            kryoRegistrations.put(classTag, proxy.kryoRegistration);
        }
    }





    public LinkedHashMap<String, KryoRegistration> getKryoRegistrations() {
        return kryoRegistrations;
    }

    private static class KryoRegistrationSerializationProxy<RC> implements IOReadableWritable{
        private ClassLoader userCodeClassLoader;

        private KryoRegistration kryoRegistration;

        public KryoRegistrationSerializationProxy(ClassLoader userCodeClassLoader) {
            this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
        }

        public KryoRegistrationSerializationProxy(KryoRegistration kryoRegistration) {
            this.kryoRegistration = Preconditions.checkNotNull(kryoRegistration);
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeUTF(kryoRegistration.getRegisteredClass().getName());

            final KryoRegistration.SerializerDefinitionType serializerDefinitionType = kryoRegistration.getSerializerDefinitionType();

            out.writeInt(serializerDefinitionType.ordinal());
            switch (serializerDefinitionType) {
                case UNSPECIFIED:
                    // nothing else to write
                    break;
                case CLASS:
                    out.writeUTF(kryoRegistration.getSerializerClass().getName());
                    break;
                case INSTANCE:
                    try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out)) {
                        InstantiationUtil.serializeObject(outViewWrapper, kryoRegistration.getSerializableSerializerInstance());
                    }
                    break;
                default:
                    // this should not happen; adding as a guard for the future
                    throw new IllegalStateException(
                            "Unrecognized Kryo registration serializer definition type: " + serializerDefinitionType);
            }
        }
        @SuppressWarnings("unchecked")
        @Override
        public void read(DataInputView in) throws IOException {
            String registeredClassname = in.readUTF();

            Class<RC> registeredClass;
            try {
                registeredClass = (Class<RC>) Class.forName(registeredClassname, true, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                LOG.warn("Cannot find registered class " + registeredClassname + " for Kryo serialization in classpath;" +
                        " using a dummy class as a placeholder.", e);

                registeredClass = (Class) DummyRegisteredClass.class;
            }

            final KryoRegistration.SerializerDefinitionType serializerDefinitionType =
                    KryoRegistration.SerializerDefinitionType.values()[in.readInt()];

            switch (serializerDefinitionType) {
                case UNSPECIFIED:
                    kryoRegistration = new KryoRegistration(registeredClass);
                    break;

                case CLASS:
                    String serializerClassname = in.readUTF();

                    Class serializerClass;
                    try {
                        serializerClass = Class.forName(serializerClassname, true, userCodeClassLoader);
                    } catch (ClassNotFoundException e) {
                        LOG.warn("Cannot find registered Kryo serializer class for class " + registeredClassname +
                                " in classpath; using a dummy Kryo serializer that should be replaced as soon as" +
                                " a new Kryo serializer for the class is present", e);

                        serializerClass = DummyKryoSerializerClass.class;
                    }

                    kryoRegistration = new KryoRegistration(registeredClass, serializerClass);
                    break;

                case INSTANCE:
                    ExecutionConfig.SerializableSerializer<? extends Serializer<RC>> serializerInstance;

                    try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
                        serializerInstance = InstantiationUtil.deserializeObject(inViewWrapper, userCodeClassLoader);
                    } catch (ClassNotFoundException e) {
                        LOG.warn("Cannot find registered Kryo serializer class for class " + registeredClassname +
                                " in classpath; using a dummy Kryo serializer that should be replaced as soon as" +
                                " a new Kryo serializer for the class is present", e);

                        serializerInstance = new ExecutionConfig.SerializableSerializer<>(new DummyKryoSerializerClass<RC>());
                    } catch (InvalidClassException e) {
                        LOG.warn("The registered Kryo serializer class for class " + registeredClassname +
                                " has changed and is no longer valid; using a dummy Kryo serializer that should be replaced" +
                                " as soon as a new Kryo serializer for the class is present.", e);

                        serializerInstance = new ExecutionConfig.SerializableSerializer<>(new DummyKryoSerializerClass<RC>());
                    }

                    kryoRegistration = new KryoRegistration(registeredClass, serializerInstance);
                    break;

                default:
                    // this should not happen; adding as a guard for the future
                    throw new IllegalStateException(
                            "Unrecognized Kryo registration serializer definition type: " + serializerDefinitionType);
            }
        }

    }






    public static class DummyRegisteredClass implements Serializable {}
    public static class DummyKryoSerializerClass<RC> extends Serializer<RC> implements Serializable
    {
        private static final long serialVersionUID = -6172780797425739308L;

        @Override
        public void write(Kryo kryo, Output output, Object o){
            throw new UnsupportedOperationException(
                    "This exception indicates that you're trying to write a data type" +
                            " that no longer has a valid Kryo serializer registered for it.");
        }

        @Override
        public Object read(Kryo kryo, Input input, Class aClass) {
            throw new UnsupportedOperationException(
                    "This exception indicates that you're trying to read a data type" +
                            " that no longer has a valid Kryo serializer registered for it.");
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
                && kryoRegistrations.equals(((KryoSerializer.KryoSerializerConfigSnapshot) obj).getKryoRegistrations());
    }

    @Override
    public int hashCode() {
        return super.hashCode() + kryoRegistrations.hashCode();
    }


}
