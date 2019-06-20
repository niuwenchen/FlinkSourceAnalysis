package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/17.
 */
public abstract class TypeComparator<T>  implements Serializable {
    private static final long serialVersionUID = 1L;

    public abstract int hash(T record);

    public abstract void setReference(T toCompare);

    public abstract boolean equalToReference(T candidate);
    public abstract int compareToReference(TypeComparator<T> referencedComparator);

    // A special case method that the runtime uses for special "PactRecord" support
    public boolean supportsCompareAgainstReference() {
        return false;
    }

    public abstract int compare(T first, T second);

    public abstract int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException;

    public abstract boolean supportsNormalizedKey();
    public abstract boolean supportsSerializationWithKeyNormalization();

    public abstract int getNormalizeKeyLen();

    public abstract boolean isNormalizedKeyPrefixOnly(int keyBytes);

    public abstract void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes);

    public abstract void writeWithKeyNormalization(T record, DataOutputView target) throws IOException;

    public abstract T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException;

    public abstract boolean invertNormalizedKey();

    public abstract TypeComparator<T> duplicate();
    public abstract int extractKeys(Object record, Object[] target, int index);

    @SuppressWarnings("rawtypes")
    public abstract TypeComparator[] getFlatComparators();

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("rawtypes")
    public int compareAgainstReference(Comparable[] keys) {
        throw new UnsupportedOperationException("Workaround hack.");
    }




}
