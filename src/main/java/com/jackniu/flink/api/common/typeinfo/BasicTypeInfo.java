package com.jackniu.flink.api.common.typeinfo;


import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.common.typeutils.base.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/6/17.
 */
public class BasicTypeInfo<T> extends TypeInformation<T> implements AtomicType<T>  {
    private static final long serialVersionUID = -430955220409131770L;


    public static final BasicTypeInfo<String> STRING_TYPE_INFO = new BasicTypeInfo<>(String.class, new Class<?>[]{}, StringSerializer.INSTANCE, StringComparator.class);
    public static final BasicTypeInfo<Boolean> BOOLEAN_TYPE_INFO = new BasicTypeInfo<>(Boolean.class, new Class<?>[]{}, BooleanSerializer.INSTANCE, BooleanComparator.class);
//    public static final BasicTypeInfo<Byte> BYTE_TYPE_INFO = new IntegerTypeInfo<>(Byte.class, new Class<?>[]{Short.class, Integer.class, Long.class, Float.class, Double.class, Character.class}, ByteSerializer.INSTANCE, ByteComparator.class);
//    public static final BasicTypeInfo<Short> SHORT_TYPE_INFO = new IntegerTypeInfo<>(Short.class, new Class<?>[]{Integer.class, Long.class, Float.class, Double.class, Character.class}, DefaultSerializers.ShortSerializer.INSTANCE, ShortComparator.class);
//    public static final BasicTypeInfo<Integer> INT_TYPE_INFO = new IntegerTypeInfo<>(Integer.class, new Class<?>[]{Long.class, Float.class, Double.class, Character.class}, DefaultSerializers.IntSerializer.INSTANCE, IntComparator.class);
//    public static final BasicTypeInfo<Long> LONG_TYPE_INFO = new IntegerTypeInfo<>(Long.class, new Class<?>[]{Float.class, Double.class, Character.class}, LongSerializer.INSTANCE, LongComparator.class);
//    public static final BasicTypeInfo<Float> FLOAT_TYPE_INFO = new FractionalTypeInfo<>(Float.class, new Class<?>[]{Double.class}, FloatSerializer.INSTANCE, FloatComparator.class);
//    public static final BasicTypeInfo<Double> DOUBLE_TYPE_INFO = new FractionalTypeInfo<>(Double.class, new Class<?>[]{}, DoubleSerializer.INSTANCE, DoubleComparator.class);
//    public static final BasicTypeInfo<Character> CHAR_TYPE_INFO = new BasicTypeInfo<>(Character.class, new Class<?>[]{}, CharSerializer.INSTANCE, CharComparator.class);
//    public static final BasicTypeInfo<Date> DATE_TYPE_INFO = new BasicTypeInfo<>(Date.class, new Class<?>[]{}, DateSerializer.INSTANCE, DateComparator.class);
//    public static final BasicTypeInfo<Void> VOID_TYPE_INFO = new BasicTypeInfo<>(Void.class, new Class<?>[]{}, VoidSerializer.INSTANCE, null);
//    public static final BasicTypeInfo<BigInteger> BIG_INT_TYPE_INFO = new BasicTypeInfo<>(BigInteger.class, new Class<?>[]{}, BigIntSerializer.INSTANCE, BigIntComparator.class);
//    public static final BasicTypeInfo<BigDecimal> BIG_DEC_TYPE_INFO = new BasicTypeInfo<>(BigDecimal.class, new Class<?>[]{}, BigDecSerializer.INSTANCE, BigDecComparator.class);
//    public static final BasicTypeInfo<Instant> INSTANT_TYPE_INFO = new BasicTypeInfo<>(Instant.class, new Class<?>[]{}, InstantSerializer.INSTANCE, InstantComparator.class);


    // --------------------------------------------------------------------------------------------

    private final Class<T> clazz;

    private final TypeSerializer<T> serializer;

    private final Class<?>[] possibleCastTargetTypes;

    private final Class<? extends TypeComparator<T>> comparatorClass;

    protected BasicTypeInfo(Class<T> clazz, Class<?>[] possibleCastTargetTypes, TypeSerializer<T> serializer, Class<? extends TypeComparator<T>> comparatorClass) {
        this.clazz = checkNotNull(clazz);
        this.possibleCastTargetTypes = checkNotNull(possibleCastTargetTypes);
        this.serializer = checkNotNull(serializer);
        // comparator can be null as in VOID_TYPE_INFO
        this.comparatorClass = comparatorClass;
    }

    @PublicEvolving
    public boolean shouldAutocastTo(BasicTypeInfo<?> to) {
        for (Class<?> possibleTo: possibleCastTargetTypes) {
            if (possibleTo.equals(to.getTypeClass())) {
                return true;
            }
        }
        return false;
    }

    @Override
    @PublicEvolving
    public boolean isBasicType() {
        return true;
    }

    @Override
    @PublicEvolving
    public boolean isTupleType() {
        return false;
    }

    @Override
    @PublicEvolving
    public int getArity() {
        return 1;
    }

    @Override
    @PublicEvolving
    public int getTotalFields() {
        return 1;
    }

    @Override
    @PublicEvolving
    public Class<T> getTypeClass() {
        return this.clazz;
    }

    @Override
    @PublicEvolving
    public boolean isKeyType() {
        return true;
    }

    @Override
    @PublicEvolving
    public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        return this.serializer;
    }

}
