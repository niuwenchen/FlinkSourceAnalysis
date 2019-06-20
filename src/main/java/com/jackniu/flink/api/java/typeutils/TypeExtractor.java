package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.functions.FlatMapFunction;
import com.jackniu.flink.api.common.functions.Function;
import com.jackniu.flink.api.common.functions.InvalidTypesException;
import com.jackniu.flink.api.common.functions.MapFunction;
import com.jackniu.flink.api.common.typeinfo.TypeInfoFactory;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by JackNiu on 2019/6/20.
 */
public class TypeExtractor {
    /** The name of the class representing Hadoop's writable */
    private static final String HADOOP_WRITABLE_CLASS = "org.apache.hadoop.io.Writable";

    private static final String HADOOP_WRITABLE_TYPEINFO_CLASS = "org.apache.flink.api.java.typeutils.WritableTypeInfo";

    private static final String AVRO_SPECIFIC_RECORD_BASE_CLASS = "org.apache.avro.specific.SpecificRecordBase";

    private static final Logger LOG = LoggerFactory.getLogger(TypeExtractor.class);

    public static final int[] NO_INDEX = new int[] {};

    protected TypeExtractor() {
        // only create instances for special use cases
    }

    // --------------------------------------------------------------------------------------------
    //  TypeInfoFactory registry
    // --------------------------------------------------------------------------------------------

    private static Map<Type, Class<? extends TypeInfoFactory>> registeredTypeInfoFactories = new HashMap<>();

    private static void registerFactory(Type t, Class<? extends TypeInfoFactory> factory) {
        Preconditions.checkNotNull(t, "Type parameter must not be null.");
        Preconditions.checkNotNull(factory, "Factory parameter must not be null.");

        if (!TypeInfoFactory.class.isAssignableFrom(factory)) {
            throw new IllegalArgumentException("Class is not a TypeInfoFactory.");
        }
        if (registeredTypeInfoFactories.containsKey(t)) {
            throw new InvalidTypesException("A TypeInfoFactory for type '" + t + "' is already registered.");
        }
        registeredTypeInfoFactories.put(t, factory);
    }


    @PublicEvolving
    public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapInterface, TypeInformation<IN> inType) {
        return getMapReturnTypes(mapInterface, inType, null, false);
    }

    @PublicEvolving
    public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapInterface, TypeInformation<IN> inType,
                                                                   String functionName, boolean allowMissing)
    {
        return getUnaryOperatorReturnType(
                (Function) mapInterface,
                MapFunction.class,
                0,
                1,
                NO_INDEX,
                inType,
                functionName,
                allowMissing);
    }


    @PublicEvolving
    public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapInterface, TypeInformation<IN> inType) {
        return getFlatMapReturnTypes(flatMapInterface, inType, null, false);
    }

    @PublicEvolving
    public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapInterface, TypeInformation<IN> inType,
                                                                       String functionName, boolean allowMissing)
    {
        return getUnaryOperatorReturnType(
                (Function) flatMapInterface,
                FlatMapFunction.class,
                0,
                1,
                new int[]{1, 0},
                inType,
                functionName,
                allowMissing);
    }



}
