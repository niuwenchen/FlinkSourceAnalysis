package com.jackniu.flink.api.java.typeutils.runtime.kyro;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.jackniu.flink.annotations.VisibleForTesting;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeutils.*;
import com.jackniu.flink.api.java.typeutils.AvroUtils;
import com.jackniu.flink.api.java.typeutils.runtime.*;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.util.InstantiationUtil;
import org.apache.commons.lang3.exception.CloneFailedException;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/6/26.
 */
public class KryoSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 3L;

    private static final Logger LOG = LoggerFactory.getLogger(KryoSerializer.class);

    /** Flag whether to check for concurrent thread access.
     * Because this flag is static final, a value of 'false' allows the JIT compiler to eliminate
     * the guarded code sections. */
    private static final boolean CONCURRENT_ACCESS_CHECK =
            LOG.isDebugEnabled() || KryoSerializerDebugInitHelper.setToDebug;

    static {
        configureKryoLogging();
    }

    // ------------------------------------------------------------------------

    private final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> defaultSerializers;
    private final LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses;

    /**
     * Map of class tag (using classname as tag) to their Kryo registration.
     *
     * <p>This map serves as a preview of the final registration result of
     * the Kryo instance, taking into account registration overwrites.
     */
    private LinkedHashMap<String, KryoRegistration> kryoRegistrations;

    private final Class<T> type;

    // ------------------------------------------------------------------------
    // The fields below are lazily initialized after duplication or deserialization.

    private transient Kryo kryo;
    private transient T copyInstance;

    private transient DataOutputView previousOut;
    private transient DataInputView previousIn;

    private transient Input input;
    private transient Output output;


    private LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> registeredTypesWithSerializers;
    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> registeredTypesWithSerializerClasses;
    private LinkedHashSet<Class<?>> registeredTypes;

    // for debugging purposes
    private transient volatile Thread currentThread;

    public KryoSerializer(Class<T> type, ExecutionConfig executionConfig) {
        this.type = checkNotNull(type);

        this.defaultSerializers = executionConfig.getDefaultKryoSerializers();
        this.defaultSerializerClasses = executionConfig.getDefaultKryoSerializerClasses();

        this.kryoRegistrations = buildKryoRegistrations(
                this.type,
                executionConfig.getRegisteredKryoTypes(),
                executionConfig.getRegisteredTypesWithKryoSerializerClasses(),
                executionConfig.getRegisteredTypesWithKryoSerializers()
        );
    }
    protected KryoSerializer(KryoSerializer<T> toCopy) {
        this.type = checkNotNull(toCopy.type, "Type class cannot be null.");
        this.defaultSerializerClasses = toCopy.defaultSerializerClasses;
        this.defaultSerializers = new LinkedHashMap<>(toCopy.defaultSerializers.size());
        this.kryoRegistrations = new LinkedHashMap<>(toCopy.kryoRegistrations.size());

        for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> entry : toCopy.defaultSerializers.entrySet()) {
            this.defaultSerializers.put(entry.getKey(), deepCopySerializer(entry.getValue()));
        }
        // deep copy the serializer instances in kryoRegistrations
        for (Map.Entry<String, KryoRegistration> entry : toCopy.kryoRegistrations.entrySet()) {
            KryoRegistration kryoRegistration = entry.getValue();

            if (kryoRegistration.getSerializerDefinitionType() == KryoRegistration.SerializerDefinitionType.INSTANCE) {

                ExecutionConfig.SerializableSerializer<? extends Serializer<?>> serializerInstance =
                        kryoRegistration.getSerializableSerializerInstance();

                if (serializerInstance != null) {
                    kryoRegistration = new KryoRegistration(
                            kryoRegistration.getRegisteredClass(),
                            deepCopySerializer(serializerInstance));
                }
            }

            this.kryoRegistrations.put(entry.getKey(), kryoRegistration);

        }
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new KryoSerializer<T>(this);
    }

    @Override
    public T createInstance() {
       if (Modifier.isAbstract(type.getModifiers()) || Modifier.isInterface(type.getModifiers())){
           return null;
       }else{
           checkKryoInitialized();
           try{
               return kryo.newInstance(type);
           }catch (Throwable e){
               return null;
           }
       }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T copy(T from) {
        if (from == null) return  null;
        if(CONCURRENT_ACCESS_CHECK){
            enterExclusiveThread();
        }
        try {
            checkKryoInitialized();
            try {
                return kryo.copy(from);
            } catch (KryoException ke) {
                // kryo was unable to copy it, so we do it through serialization:
                ByteArrayOutputStream baout = new ByteArrayOutputStream();
                Output output = new Output(baout);

                kryo.writeObject(output, from);

                output.close();

                ByteArrayInputStream bain = new ByteArrayInputStream(baout.toByteArray());
                Input input = new Input(bain);

                return (T) kryo.readObject(input, from.getClass());
            }
        }finally {
            if (CONCURRENT_ACCESS_CHECK) {
                exitExclusiveThread();
            }
        }

    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (CONCURRENT_ACCESS_CHECK) {
            enterExclusiveThread();
        }

        try {
            checkKryoInitialized();

            if (target != previousOut) {
                DataOutputViewStream outputStream = new DataOutputViewStream(target);
                output = new Output(outputStream);
                previousOut = target;
            }

            // Sanity check: Make sure that the output is cleared/has been flushed by the last call
            // otherwise data might be written multiple times in case of a previous EOFException
            if (output.position() != 0) {
                throw new IllegalStateException("The Kryo Output still contains data from a previous " +
                        "serialize call. It has to be flushed or cleared at the end of the serialize call.");
            }

            try {
                kryo.writeClassAndObject(output, record);
                output.flush();
            }
            catch (KryoException ke) {
                // make sure that the Kryo output buffer is cleared in case that we can recover from
                // the exception (e.g. EOFException which denotes buffer full)
                output.clear();

                Throwable cause = ke.getCause();
                if (cause instanceof EOFException) {
                    throw (EOFException) cause;
                }
                else {
                    throw ke;
                }
            }
        }
        finally {
            if (CONCURRENT_ACCESS_CHECK) {
                exitExclusiveThread();
            }
        }
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        if (CONCURRENT_ACCESS_CHECK) {
            enterExclusiveThread();
        }

        try {
            checkKryoInitialized();

            if (source != previousIn) {
                DataInputViewStream inputStream = new DataInputViewStream(source);
                input = new NoFetchingInput(inputStream);
                previousIn = source;
            }

            try {
                return (T) kryo.readClassAndObject(input);
            } catch (KryoException ke) {
                Throwable cause = ke.getCause();

                if (cause instanceof EOFException) {
                    throw (EOFException) cause;
                } else {
                    throw ke;
                }
            }
        }
        finally {
            if (CONCURRENT_ACCESS_CHECK) {
                exitExclusiveThread();
            }
        }
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {

    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KryoSerializer) {
            KryoSerializer<?> other = (KryoSerializer<?>) obj;

            return other.canEqual(this) &&
                    type == other.type &&
                    Objects.equals(kryoRegistrations, other.kryoRegistrations) &&
                    Objects.equals(defaultSerializerClasses, other.defaultSerializerClasses) &&
                    Objects.equals(defaultSerializers, other.defaultSerializers);
        } else {
            return false;
        }
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof KryoSerializer;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (kryoRegistrations.hashCode());
        result = 31 * result + (defaultSerializers.hashCode());
        result = 31 * result + (defaultSerializerClasses.hashCode());

        return result;
    }

    /**
     * Returns the Chill Kryo Serializer which is implicitly added to the classpath via flink-runtime.
     * Falls back to the default Kryo serializer if it can't be found.
     * @return The Kryo serializer instance.
     */
    private Kryo getKryoInstance() {
        try{
            Class<?> chillInstantiatorClazz =
                    Class.forName("org.apache.flink.runtime.types.FlinkScalaKryoInstantiator");
            Object chillInstantiator = chillInstantiatorClazz.newInstance();

            // obtain a Kryo instance through Twitter Chill
            Method m = chillInstantiatorClazz.getMethod("newKryo");

            return (Kryo) m.invoke(chillInstantiator);

        } catch (ClassNotFoundException | InstantiationException | NoSuchMethodException |
                IllegalAccessException | InvocationTargetException e) {
            LOG.warn("Falling back to default Kryo serializer because Chill serializer couldn't be found.", e);

            Kryo.DefaultInstantiatorStrategy initStrategy = new Kryo.DefaultInstantiatorStrategy();
            initStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

            Kryo kryo = new Kryo();
            kryo.setInstantiatorStrategy(initStrategy);

            return kryo;
        }
    }
    private void checkKryoInitialized() {
        if (this.kryo == null){
            this.kryo =getKryoInstance();

            kryo.setReferences(true);

            // Throwable and all subclasses should be serialized via java serialization
            // Note: the registered JavaSerializer is Flink's own implementation, and not Kryo's.
            //       This is due to a know issue with Kryo's JavaSerializer. See FLINK-6025 for details.
            kryo.addDefaultSerializer(Throwable.class,new JavaSerializer());


            for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> entry: defaultSerializers.entrySet()) {
                kryo.addDefaultSerializer(entry.getKey(), entry.getValue().getSerializer());
            }

            for (Map.Entry<Class<?>, Class<? extends Serializer<?>>> entry: defaultSerializerClasses.entrySet()) {
                kryo.addDefaultSerializer(entry.getKey(), entry.getValue());
            }

            KryoUtils.applyRegistrations(this.kryo, kryoRegistrations.values());

            kryo.setRegistrationRequired(false);
            kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        }
    }

    @Override
    public KryoSerializerConfigSnapshot<T> snapshotConfiguration() {
        return new KryoSerializerConfigSnapshot<>(type, kryoRegistrations);
    }

    @Override
    public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
        if (configSnapshot instanceof KryoSerializerConfigSnapshot) {
            final KryoSerializerConfigSnapshot<T> config = (KryoSerializerConfigSnapshot<T>) configSnapshot;

            if (type.equals(config.getTypeClass())) {
                LinkedHashMap<String, KryoRegistration> reconfiguredRegistrations = config.getKryoRegistrations();
                reconfiguredRegistrations.putAll(kryoRegistrations);
                for (Map.Entry<String, KryoRegistration> reconfiguredRegistrationEntry : reconfiguredRegistrations.entrySet()) {
                    if (reconfiguredRegistrationEntry.getValue().isDummy()) {
                        LOG.warn("The Kryo registration for a previously registered class {} does not have a " +
                                "proper serializer, because its previous serializer cannot be loaded or is no " +
                                "longer valid but a new serializer is not available", reconfiguredRegistrationEntry.getKey());

                        return CompatibilityResult.requiresMigration();
                    }
                }
                this.kryoRegistrations = reconfiguredRegistrations;
                return CompatibilityResult.compatible();
            }
        }
        return CompatibilityResult.requiresMigration();
    }

    public static final class KryoSerializerConfigSnapshot<T> extends KryoRegistrationSerializerConfigSnapshot<T> {
        private static final int VERSION = 1;

        /** This empty nullary constructor is required for deserializing the configuration. */
        public KryoSerializerConfigSnapshot() {}

        public KryoSerializerConfigSnapshot(
                Class<T> typeClass,
                LinkedHashMap<String, KryoRegistration> kryoRegistrations) {

            super(typeClass, kryoRegistrations);
        }
        @Override
        public int getVersion() {
            return VERSION;
        }

    }

    private static LinkedHashMap<String, KryoRegistration> buildKryoRegistrations(
            Class<?> serializedType,
            LinkedHashSet<Class<?>> registeredTypes,
            LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> registeredTypesWithSerializerClasses,
            LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> registeredTypesWithSerializers) {

        final LinkedHashMap<String, KryoRegistration> kryoRegistrations = new LinkedHashMap<>();

        kryoRegistrations.put(serializedType.getName(), new KryoRegistration(serializedType));

        for (Class<?> registeredType : checkNotNull(registeredTypes)) {
            kryoRegistrations.put(registeredType.getName(), new KryoRegistration(registeredType));
        }

        for (Map.Entry<Class<?>, Class<? extends Serializer<?>>> registeredTypeWithSerializerClassEntry :
                checkNotNull(registeredTypesWithSerializerClasses).entrySet()) {

            kryoRegistrations.put(
                    registeredTypeWithSerializerClassEntry.getKey().getName(),
                    new KryoRegistration(
                            registeredTypeWithSerializerClassEntry.getKey(),
                            registeredTypeWithSerializerClassEntry.getValue()));
        }

        for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> registeredTypeWithSerializerEntry :
                checkNotNull(registeredTypesWithSerializers).entrySet()) {

            kryoRegistrations.put(
                    registeredTypeWithSerializerEntry.getKey().getName(),
                    new KryoRegistration(
                            registeredTypeWithSerializerEntry.getKey(),
                            registeredTypeWithSerializerEntry.getValue()));
        }

        // add Avro support if flink-avro is available; a dummy otherwise
        AvroUtils.getAvroUtils().addAvroGenericDataArrayRegistration(kryoRegistrations);

        return kryoRegistrations;
    }

    static void configureKryoLogging() {
        // Kryo uses only DEBUG and TRACE levels
        // we only forward TRACE level, because even DEBUG levels results in
        // a logging for each object, which is infeasible in Flink.
        if (LOG.isTraceEnabled()) {
            com.esotericsoftware.minlog.Log.setLogger(new MinlogForwarder(LOG));
            com.esotericsoftware.minlog.Log.TRACE();
        }
    }
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // kryoRegistrations may be null if this Kryo serializer is deserialized from an old version
        if (kryoRegistrations == null) {
            this.kryoRegistrations = buildKryoRegistrations(
                    type,
                    registeredTypes,
                    registeredTypesWithSerializerClasses,
                    registeredTypesWithSerializers);
        }
    }

    private ExecutionConfig.SerializableSerializer<? extends Serializer<?>> deepCopySerializer(
            ExecutionConfig.SerializableSerializer<? extends Serializer<?>> original) {
        try {
            return InstantiationUtil.clone(original, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException ex) {
            throw new CloneFailedException(
                    "Could not clone serializer instance of class " + original.getClass(),
                    ex);
        }
    }

    // --------------------------------------------------------------------------------------------
    // For testing
    // --------------------------------------------------------------------------------------------

    private void enterExclusiveThread() {
        // we use simple get, check, set here, rather than CAS
        // we don't need lock-style correctness, this is only a sanity-check and we thus
        // favor speed at the cost of some false negatives in this check
        Thread previous = currentThread;
        Thread thisThread = Thread.currentThread();

        if (previous == null) {
            currentThread = thisThread;
        }
        else if (previous != thisThread) {
            throw new IllegalStateException(
                    "Concurrent access to KryoSerializer. Thread 1: " + thisThread.getName() +
                            " , Thread 2: " + previous.getName());
        }
    }

    private void exitExclusiveThread() {
        currentThread = null;
    }

    @VisibleForTesting
    public Kryo getKryo() {
        checkKryoInitialized();
        return this.kryo;
    }
}

