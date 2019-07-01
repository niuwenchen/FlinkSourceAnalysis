package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.annotations.Public;
import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.java.tuple.Tuple;
import com.jackniu.flink.api.java.tuple.Tuple0;
import com.jackniu.flink.api.java.typeutils.runtime.Tuple0Serializer;
import com.jackniu.flink.api.java.typeutils.runtime.TupleSerializer;

import java.util.ArrayList;
import java.util.Collections;

import static com.jackniu.flink.util.Preconditions.checkArgument;
import static com.jackniu.flink.util.Preconditions.checkState;
/**
 * Created by JackNiu on 2019/6/25.
 */
public final class TupleTypeInfo<T extends Tuple> extends TupleTypeInfoBase<T> {
    private static final long serialVersionUID = 1L;

    protected final String[] fieldNames;

    @SuppressWarnings("unchecked")
    @PublicEvolving
    public TupleTypeInfo(TypeInformation<?>... types) {
        this((Class<T>) Tuple.getTupleClass(types.length), types);
    }

    @PublicEvolving
    public TupleTypeInfo(Class<T>tupleType,TypeInformation<?>... types){
        super(tupleType,types);
        checkArgument(
                types.length <= Tuple.MAX_ARITY,
                "The tuple type exceeds the maximum supported arity.");

        this.fieldNames = new String[types.length];
        for(int i=0;i<types.length;i++){
            fieldNames[i] = "f"+i;
        }
    }
    @Override
    @PublicEvolving
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    @PublicEvolving
    public int getFieldIndex(String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    @SuppressWarnings("unchecked")
    @Override
    @PublicEvolving
    public TupleSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        if (getTypeClass() == Tuple0.class) {
            return (TupleSerializer<T>) Tuple0Serializer.INSTANCE;
        }

        TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[getArity()];
        for (int i = 0; i < types.length; i++) {
            fieldSerializers[i] = types[i].createSerializer(executionConfig);
        }

        Class<T> tupleClass = getTypeClass();

        return new TupleSerializer<T>(tupleClass, fieldSerializers);
    }


    @Override
    protected TypeComparatorBuilder<T> createTypeComparatorBuilder() {
        return new TupleTypeComparatorBuilder();
    }

    private class TupleTypeComparatorBuilder implements TypeComparatorBuilder<T> {

        private final ArrayList<TypeComparator> fieldComparators = new ArrayList<TypeComparator>();
        private final ArrayList<Integer> logicalKeyFields = new ArrayList<Integer>();

        @Override
        public void initializeTypeComparatorBuilder(int size) {
            fieldComparators.ensureCapacity(size);
            logicalKeyFields.ensureCapacity(size);
        }

        @Override
        public void addComparatorField(int fieldId, TypeComparator<?> comparator) {
            fieldComparators.add(comparator);
            logicalKeyFields.add(fieldId);
        }

        @Override
        public TypeComparator<T> createTypeComparator(ExecutionConfig config) {
            checkState(
                    fieldComparators.size() > 0,
                    "No field comparators were defined for the TupleTypeComparatorBuilder."
            );

            checkState(
                    logicalKeyFields.size() > 0,
                    "No key fields were defined for the TupleTypeComparatorBuilder."
            );

            checkState(
                    fieldComparators.size() == logicalKeyFields.size(),
                    "The number of field comparators and key fields is not equal."
            );

            final int maxKey = Collections.max(logicalKeyFields);

            checkState(
                    maxKey >= 0,
                    "The maximum key field must be greater or equal than 0."
            );

            TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1];

            for (int i = 0; i <= maxKey; i++) {
                fieldSerializers[i] = types[i].createSerializer(config);
            }

            return new TupleComparator<T>(
                    listToPrimitives(logicalKeyFields),
                    fieldComparators.toArray(new TypeComparator[fieldComparators.size()]),
                    fieldSerializers
            );
        }
    }


}
