package com.jackniu.flink.api.java.typeutils.runtime;

import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.core.memory.MemorySegment;
import com.jackniu.flink.types.NormalizableKey;
import com.jackniu.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/28.
 */
public class GenericTypeComparator<T extends Comparable<T>> extends TypeComparator<T> {
    private static final long serialVersionUID = 1L;

    private final boolean ascending;

    private final Class<T> type;

    private TypeSerializer<T> serializer;

    private transient T reference;

    private transient T tmpReference;

    @SuppressWarnings("rawtypes")
    private final TypeComparator[] comparators = new TypeComparator[] {this};

    public GenericTypeComparator(boolean ascending, TypeSerializer<T> serializer,Class<T> type){
        this.ascending = ascending;
        this.serializer = serializer;
        this.type = type;
    }

    private GenericTypeComparator(GenericTypeComparator<T> toClone) {
        this.ascending = toClone.ascending;
        this.serializer = toClone.serializer.duplicate();
        this.type = toClone.type;
    }

    @Override
    public int hash(T record) {
        return record.hashCode();
    }

    @Override
    public void setReference(T toCompare) {
        this.reference = this.serializer.copy(toCompare);
    }

    @Override
    public boolean equalToReference(T candidate) {
        return candidate.equals(this.reference);
    }

    @Override
    public int compareToReference(TypeComparator<T> referencedComparator) {
        T otherRef = ((GenericTypeComparator<T>) referencedComparator).reference;
        int cmp = otherRef.compareTo(this.reference);

        return this.ascending ? cmp : -cmp;
    }

    @Override
    public int compare(T first, T second) {
        int cmp = first.compareTo(second);
        return this.ascending ? cmp : -cmp;
    }

    @Override
    public int compareSerialized(final DataInputView firstSource, final DataInputView secondSource) throws IOException {

        if (this.reference == null) {
            this.reference = this.serializer.createInstance();
        }

        if (this.tmpReference == null) {
            this.tmpReference = this.serializer.createInstance();
        }

        this.reference = this.serializer.deserialize(this.reference, firstSource);
        this.tmpReference = this.serializer.deserialize(this.tmpReference, secondSource);

        int cmp = this.reference.compareTo(this.tmpReference);
        return this.ascending ? cmp : -cmp;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return NormalizableKey.class.isAssignableFrom(this.type);
    }

    @Override
    public int getNormalizeKeyLen() {
        if (this.reference == null) {
            this.reference = InstantiationUtil.instantiate(this.type);
        }

        NormalizableKey<?> key = (NormalizableKey<?>) this.reference;
        return key.getMaxNormalizedKeyLen();
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < getNormalizeKeyLen();
    }

    @Override
    public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
        NormalizableKey<?> key = (NormalizableKey<?>) record;
        key.copyNormalizedKey(target, offset, numBytes);
    }

    @Override
    public boolean invertNormalizedKey() {
        return !ascending;
    }

    @Override
    public TypeComparator<T> duplicate() {
        return new GenericTypeComparator<T>(this);
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        target[index] = record;
        return 1;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public TypeComparator[] getFlatComparators() {
        return comparators;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
        throw new UnsupportedOperationException();
    }

}
