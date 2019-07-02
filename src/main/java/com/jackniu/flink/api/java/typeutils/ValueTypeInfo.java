package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.api.common.functions.InvalidTypesException;
import com.jackniu.flink.api.common.typeinfo.AtomicType;
import com.jackniu.flink.api.common.typeinfo.TypeInformation;
import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.common.typeutils.base.*;
import com.jackniu.flink.api.java.typeutils.runtime.CopyableValueComparator;
import com.jackniu.flink.api.java.typeutils.runtime.CopyableValueSerializer;
import com.jackniu.flink.api.java.typeutils.runtime.ValueComparator;
import com.jackniu.flink.api.java.typeutils.runtime.ValueSerializer;
import com.jackniu.flink.types.*;

import static com.jackniu.flink.util.Preconditions.checkArgument;
import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/1.
 */
public class ValueTypeInfo<T extends Value> extends TypeInformation<T> implements AtomicType<T> {
    private static final long serialVersionUID = 1L;

    public static final ValueTypeInfo<BooleanValue> BOOLEAN_VALUE_TYPE_INFO = new ValueTypeInfo<>(BooleanValue.class);
    public static final ValueTypeInfo<ByteValue> BYTE_VALUE_TYPE_INFO = new ValueTypeInfo<>(ByteValue.class);
    public static final ValueTypeInfo<CharValue> CHAR_VALUE_TYPE_INFO = new ValueTypeInfo<>(CharValue.class);
    public static final ValueTypeInfo<DoubleValue> DOUBLE_VALUE_TYPE_INFO = new ValueTypeInfo<>(DoubleValue.class);
    public static final ValueTypeInfo<FloatValue> FLOAT_VALUE_TYPE_INFO = new ValueTypeInfo<>(FloatValue.class);
    public static final ValueTypeInfo<IntValue> INT_VALUE_TYPE_INFO = new ValueTypeInfo<>(IntValue.class);
    public static final ValueTypeInfo<LongValue> LONG_VALUE_TYPE_INFO = new ValueTypeInfo<>(LongValue.class);
    public static final ValueTypeInfo<NullValue> NULL_VALUE_TYPE_INFO = new ValueTypeInfo<>(NullValue.class);
    public static final ValueTypeInfo<ShortValue> SHORT_VALUE_TYPE_INFO = new ValueTypeInfo<>(ShortValue.class);
    public static final ValueTypeInfo<StringValue> STRING_VALUE_TYPE_INFO = new ValueTypeInfo<>(StringValue.class);

    private final Class<T> type;

    @PublicEvolving
    public ValueTypeInfo(Class<T> type) {
        this.type = checkNotNull(type);

        checkArgument(
                Value.class.isAssignableFrom(type) || type.equals(Value.class),
                "ValueTypeInfo can only be used for subclasses of %s", Value.class.getName());
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
        return this.type;
    }

    @Override
    @PublicEvolving
    public boolean isBasicType() {
        return false;
    }

    @PublicEvolving
    public boolean isBasicValueType() {
        return type.equals(StringValue.class) || type.equals(ByteValue.class) || type.equals(ShortValue.class) || type.equals(CharValue.class) ||
                type.equals(DoubleValue.class) || type.equals(FloatValue.class) || type.equals(IntValue.class) || type.equals(LongValue.class) ||
                type.equals(NullValue.class) || type.equals(BooleanValue.class);
    }

    @Override
    @PublicEvolving
    public boolean isTupleType() {
        return false;
    }

    @Override
    @PublicEvolving
    public boolean isKeyType() {
        return Comparable.class.isAssignableFrom(type);
    }

    @Override
    @SuppressWarnings("unchecked")
    @PublicEvolving
    public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        if (BooleanValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) BooleanValueSerializer.INSTANCE;
        }
        else if (ByteValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) ByteValueSerializer.INSTANCE;
        }
        else if (CharValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) CharValueSerializer.INSTANCE;
        }
        else if (DoubleValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) DoubleValueSerializer.INSTANCE;
        }
        else if (FloatValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) FloatValueSerializer.INSTANCE;
        }
        else if (IntValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) IntValueSerializer.INSTANCE;
        }
        else if (LongValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) LongValueSerializer.INSTANCE;
        }
        else if (NullValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) NullValueSerializer.INSTANCE;
        }
        else if (ShortValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) ShortValueSerializer.INSTANCE;
        }
        else if (StringValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) StringValueSerializer.INSTANCE;
        }
        else if (CopyableValue.class.isAssignableFrom(type)) {
            return (TypeSerializer<T>) createCopyableValueSerializer(type.asSubclass(CopyableValue.class));
        }
        else {
            return new ValueSerializer<T>(type);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    @PublicEvolving
    public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
        if (!isKeyType()) {
            throw new RuntimeException("The type " + type.getName() + " is not Comparable.");
        }

        if (BooleanValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new BooleanValueComparator(sortOrderAscending);
        }
        else if (ByteValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new ByteValueComparator(sortOrderAscending);
        }
        else if (CharValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new CharValueComparator(sortOrderAscending);
        }
        else if (DoubleValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new DoubleValueComparator(sortOrderAscending);
        }
        else if (FloatValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new FloatValueComparator(sortOrderAscending);
        }
        else if (IntValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new IntValueComparator(sortOrderAscending);
        }
        else if (LongValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new LongValueComparator(sortOrderAscending);
        }
        else if (NullValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) NullValueComparator.getInstance();
        }
        else if (ShortValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new ShortValueComparator(sortOrderAscending);
        }
        else if (StringValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new StringValueComparator(sortOrderAscending);
        }
        else if (CopyableValue.class.isAssignableFrom(type)) {
            return (TypeComparator<T>) new CopyableValueComparator(sortOrderAscending, type);
        }
        else {
            return (TypeComparator<T>) new ValueComparator(sortOrderAscending, type);
        }
    }

    // utility method to summon the necessary bound
    private static <X extends CopyableValue<X>> CopyableValueSerializer<X> createCopyableValueSerializer(Class<X> clazz) {
        return new CopyableValueSerializer<X>(clazz);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return this.type.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ValueTypeInfo) {
            @SuppressWarnings("unchecked")
            ValueTypeInfo<T> valueTypeInfo = (ValueTypeInfo<T>) obj;

            return valueTypeInfo.canEqual(this) &&
                    type == valueTypeInfo.type;
        } else {
            return false;
        }
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof ValueTypeInfo;
    }

    @Override
    public String toString() {
        return "ValueType<" + type.getSimpleName() + ">";
    }

    // --------------------------------------------------------------------------------------------

    @PublicEvolving
    static <X extends Value> TypeInformation<X> getValueTypeInfo(Class<X> typeClass) {
        if (Value.class.isAssignableFrom(typeClass) && !typeClass.equals(Value.class)) {
            return new ValueTypeInfo<X>(typeClass);
        }
        else {
            throw new InvalidTypesException("The given class is no subclass of " + Value.class.getName());
        }
    }


}
