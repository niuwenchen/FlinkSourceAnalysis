package com.jackniu.flink.api.java.typeutils.runtime;



import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeutils.KryoRegistrationSerializerConfigSnapshot;
import com.jackniu.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/27.
 */
public class KryoRegistration implements Serializable {
    private static  final long serialVersionUID=5375110512910892655L;
    public enum SerializerDefinitionType{
        UNSPECIFIED,CLASS,INSTANCE
    }

    //The registered class
    private final Class<?> registeredClass;
    //Class of the serializer to use for registered class
    @Nullable
    private final Class<? extends Serializer<?>> serializerClass;

    @Nullable
    private final ExecutionConfig.SerializableSerializer<? extends  Serializer<?>> serializableSerializerInstance;

    private final SerializerDefinitionType serializerDefinitionType;

    public KryoRegistration(Class<?> registeredClass){
        this.registeredClass = Preconditions.checkNotNull(registeredClass);
        this.serializerClass = null;
        this.serializableSerializerInstance = null;
        this.serializerDefinitionType = SerializerDefinitionType.UNSPECIFIED;
    }
    public KryoRegistration(Class<?> registeredClass, Class<? extends Serializer<?>> serializerClass) {
        this.registeredClass = Preconditions.checkNotNull(registeredClass);

        this.serializerClass = Preconditions.checkNotNull(serializerClass);
        this.serializableSerializerInstance = null;

        this.serializerDefinitionType = SerializerDefinitionType.CLASS;
    }

    public KryoRegistration(
            Class<?> registeredClass,
            ExecutionConfig.SerializableSerializer<? extends Serializer<?>> serializableSerializerInstance) {
        this.registeredClass = Preconditions.checkNotNull(registeredClass);

        this.serializerClass = null;
        this.serializableSerializerInstance = Preconditions.checkNotNull(serializableSerializerInstance);

        this.serializerDefinitionType = SerializerDefinitionType.INSTANCE;
    }

    public Class<?> getRegisteredClass(){return this.registeredClass;}
    public SerializerDefinitionType getSerializerDefinitionType(){return serializerDefinitionType;}
    @Nullable
    public Class<? extends  Serializer<?>> getSerializerClass(){return serializerClass;}
    @Nullable
    public ExecutionConfig.SerializableSerializer<? extends Serializer<?>> getSerializableSerializerInstance() {
        return serializableSerializerInstance;
    }

    public Serializer<?> getSerializer(Kryo kryo){
        switch (serializerDefinitionType){
            case UNSPECIFIED:
                return null;
            case CLASS:
                return ReflectionSerializerFactory.makeSerializer(kryo,serializerClass,registeredClass);
            case INSTANCE:
                return serializableSerializerInstance.getSerializer();
            default:
                throw new IllegalStateException(
                        "Unrecognized Kryo registration serializer definition type: " + serializerDefinitionType);
        }
    }

    public boolean isDummy(){
        return registeredClass.equals(KryoRegistrationSerializerConfigSnapshot.DummyRegisteredClass.class)
                || (serializerClass != null)
                && serializerClass.equals(KryoRegistrationSerializerConfigSnapshot.DummyKryoSerializerClass.class)
				|| (serializableSerializerInstance != null
                && serializableSerializerInstance.getSerializer() instanceof KryoRegistrationSerializerConfigSnapshot.DummyKryoSerializerClass);
    }

    @Override
    public boolean equals(Object obj){
        if(obj == this) {
            return true;
        }

        if (obj ==  null){
            return false;
        }

        if(obj instanceof KryoRegistration){
            KryoRegistration other =(KryoRegistration) obj;
            // we cannot include the serializer instances here because they don't implement the equals method
            return serializerDefinitionType == other.serializerDefinitionType
                    && registeredClass == other.registeredClass
                    && serializerClass == other.serializerClass;

        }else{
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = serializerDefinitionType.hashCode();
        result = 31 * result + registeredClass.hashCode();

        if (serializerClass != null) {
            result = 31 * result + serializerClass.hashCode();
        }

        return result;
    }
}
