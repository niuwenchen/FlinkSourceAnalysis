package com.jackniu.flink.api.java.typeutils.runtime.kyro;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.CompositeType;
import com.jackniu.flink.api.java.typeutils.AvroUtils;
import com.jackniu.flink.api.java.typeutils.GenericTypeInfo;
import com.jackniu.flink.api.java.typeutils.ObjectArrayTypeInfo;
import com.jackniu.flink.api.java.typeutils.TypeExtractionUtils;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Created by JackNiu on 2019/6/27.
 */
public class Serializers {
    public static void recursivelyRegisterType(TypeInformation<?> typeInfo, ExecutionConfig config, Set<Class<?>> alreadySeen) {
        if (typeInfo instanceof GenericTypeInfo) {
            GenericTypeInfo<?> genericTypeInfo = (GenericTypeInfo<?>) typeInfo;
            Serializers.recursivelyRegisterType(genericTypeInfo.getTypeClass(), config, alreadySeen);
        }
        else if (typeInfo instanceof CompositeType) {
            List<GenericTypeInfo<?>> genericTypesInComposite = new ArrayList<>();
            getContainedGenericTypes((CompositeType<?>)typeInfo, genericTypesInComposite);
            for (GenericTypeInfo<?> gt : genericTypesInComposite) {
                Serializers.recursivelyRegisterType(gt.getTypeClass(), config, alreadySeen);
            }
        }
        else if (typeInfo instanceof ObjectArrayTypeInfo) {
            ObjectArrayTypeInfo<?, ?> objectArrayTypeInfo = (ObjectArrayTypeInfo<?, ?>) typeInfo;
            recursivelyRegisterType(objectArrayTypeInfo.getComponentInfo(), config, alreadySeen);
        }
    }

    public static void recursivelyRegisterType(Class<?> type, ExecutionConfig config, Set<Class<?>> alreadySeen) {
        // don't register or remember primitives
        if (type == null || type.isPrimitive() || type == Object.class) {
            return;
        }

        // prevent infinite recursion for recursive types
        if (!alreadySeen.add(type)) {
            return;
        }

        if (type.isArray()) {
            recursivelyRegisterType(type.getComponentType(), config, alreadySeen);
        }
        else {
            config.registerKryoType(type);
            // add serializers for Avro type if necessary
            AvroUtils.getAvroUtils().addAvroSerializersIfRequired(config, type);

            Field[] fields = type.getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                    continue;
                }
                Type fieldType = field.getGenericType();
                recursivelyRegisterGenericType(fieldType, config, alreadySeen);
            }
        }
    }

    private static void recursivelyRegisterGenericType(Type fieldType, ExecutionConfig config, Set<Class<?>> alreadySeen) {
        if (fieldType instanceof ParameterizedType) {
            // field has generics
            ParameterizedType parameterizedFieldType = (ParameterizedType) fieldType;

            for (Type t: parameterizedFieldType.getActualTypeArguments()) {
                if (TypeExtractionUtils.isClassType(t) ) {
                    recursivelyRegisterType(TypeExtractionUtils.typeToClass(t), config, alreadySeen);
                }
            }

            recursivelyRegisterGenericType(parameterizedFieldType.getRawType(), config, alreadySeen);
        }
        else if (fieldType instanceof GenericArrayType) {
            GenericArrayType genericArrayType = (GenericArrayType) fieldType;
            recursivelyRegisterGenericType(genericArrayType.getGenericComponentType(), config, alreadySeen);
        }
        else if (fieldType instanceof Class) {
            Class<?> clazz = (Class<?>) fieldType;
            recursivelyRegisterType(clazz, config, alreadySeen);
        }
    }

    /**
     * Returns all GenericTypeInfos contained in a composite type.
     *
     * @param typeInfo {@link CompositeType}
     */
    private static void getContainedGenericTypes(CompositeType<?> typeInfo, List<GenericTypeInfo<?>> target) {
        for (int i = 0; i < typeInfo.getArity(); i++) {
            TypeInformation<?> type = typeInfo.getTypeAt(i);
            if (type instanceof CompositeType) {
                getContainedGenericTypes((CompositeType<?>) type, target);
            } else if (type instanceof GenericTypeInfo) {
                if (!target.contains(type)) {
                    target.add((GenericTypeInfo<?>) type);
                }
            }
        }
    }

    /**
     * This is used in case we don't have Avro on the classpath. Flink versions before 1.4
     * always registered special Serializers for Kryo but starting with Flink 1.4 we don't have
     * Avro on the classpath by default anymore. We still have to retain the same registered
     * Serializers for backwards compatibility of savepoints.
     */
    public static class DummyAvroRegisteredClass {}

    /**
     * This is used in case we don't have Avro on the classpath. Flink versions before 1.4
     * always registered special Serializers for Kryo but starting with Flink 1.4 we don't have
     * Avro on the classpath by default anymore. We still have to retain the same registered
     * Serializers for backwards compatibility of savepoints.
     */
    public static class DummyAvroKryoSerializerClass<T> extends Serializer<T> {
        @Override
        public void write(Kryo kryo, Output output, Object o) {
            throw new UnsupportedOperationException("Could not find required Avro dependency.");
        }

        @Override
        public T read(Kryo kryo, Input input, Class<T> aClass) {
            throw new UnsupportedOperationException("Could not find required Avro dependency.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Custom Serializers
    // --------------------------------------------------------------------------------------------

    /**
     * Special serializer for Java's {@link ArrayList} used for Avro's GenericData.Array.
     */
    @SuppressWarnings("rawtypes")
    public static class SpecificInstanceCollectionSerializerForArrayList extends SpecificInstanceCollectionSerializer<ArrayList> {
        private static final long serialVersionUID = 1L;

        public SpecificInstanceCollectionSerializerForArrayList() {
            super(ArrayList.class);
        }
    }

    /**
     * Special serializer for Java collections enforcing certain instance types.
     * Avro is serializing collections with an "GenericData.Array" type. Kryo is not able to handle
     * this type, so we use ArrayLists.
     */
    @SuppressWarnings("rawtypes")
    public static class SpecificInstanceCollectionSerializer<T extends Collection>
            extends CollectionSerializer implements Serializable {
        private static final long serialVersionUID = 1L;

        private Class<T> type;

        public SpecificInstanceCollectionSerializer(Class<T> type) {
            this.type = type;
        }

        @Override
        protected Collection create(Kryo kryo, Input input, Class<Collection> type) {
            return kryo.newInstance(this.type);
        }

        @Override
        protected Collection createCopy(Kryo kryo, Collection original) {
            return kryo.newInstance(this.type);
        }
    }
}
