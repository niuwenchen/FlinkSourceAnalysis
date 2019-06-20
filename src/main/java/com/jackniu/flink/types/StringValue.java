package com.jackniu.flink.types;

import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.core.memory.MemorySegment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.CharBuffer;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/6/19.
 */
public class StringValue implements NormalizableKey<StringValue>, CharSequence, ResettableValue<StringValue>,
        CopyableValue<StringValue>, Appendable{

    private static final long serialVersionUID = 1L;

    private static final char[] EMPTY_STRING = new char[0];

    private static final int HIGH_BIT = 0x1 << 7;

    private static final int HIGH_BIT2 = 0x1 << 13;

    private static final int HIGH_BIT2_MASK = 0x3 << 6;


    private char[] value;		// character value of the string value, not necessarily completely filled

    private int len;			// length of the string value

    private int hashCode;		// cache for the hashCode
    public StringValue() {
        this.value = EMPTY_STRING;
    }

    public StringValue(CharSequence value) {
        this.value = EMPTY_STRING;
        setValue(value);
    }
    public StringValue(StringValue value) {
        this.value = EMPTY_STRING;
        setValue(value);
    }
    public StringValue(StringValue value, int offset, int len) {
        this.value = EMPTY_STRING;
        setValue(value, offset, len);
    }
    public void setLength(int len) {
        if (len < 0 || len > this.len) {
            throw new IllegalArgumentException("Length must be between 0 and the current length.");
        }
        this.len = len;
    }

    public char[] getCharArray() {
        return this.value;
    }
    public String getValue() {
        return toString();
    }
    public void setValue(CharSequence value) {
        checkNotNull(value);
        setValue(value, 0, value.length());
    }
    @Override
    public void setValue(StringValue value) {
        checkNotNull(value);
        setValue(value.value, 0, value.len);
    }
    public void setValue(StringValue value, int offset, int len) {
        checkNotNull(value);
        setValue(value.value, offset, len);
    }
    public void setValue(CharSequence value, int offset, int len) {
        checkNotNull(value);
        if (offset < 0 || len < 0 || offset > value.length() - len) {
            throw new IndexOutOfBoundsException("offset: " + offset + " len: " + len + " value.len: " + len);
        }

        ensureSize(len);
        this.len = len;
        for (int i = 0; i < len; i++) {
            this.value[i] = value.charAt(offset + i);
        }
        this.hashCode = 0;
    }
    public void setValue(CharBuffer buffer) {
        checkNotNull(buffer);
        final int len = buffer.length();
        ensureSize(len);
        buffer.get(this.value, 0, len);
        this.len = len;
        this.hashCode = 0;
    }

    public void setValue(char[] chars, int offset, int len) {
        checkNotNull(chars);
        if (offset < 0 || len < 0 || offset > chars.length - len) {
            throw new IndexOutOfBoundsException();
        }

        ensureSize(len);
        System.arraycopy(chars, offset, this.value, 0, len);
        this.len = len;
        this.hashCode = 0;
    }
    public void setValueAscii(byte[] bytes, int offset, int len) {
        if (bytes == null) {
            throw new NullPointerException("Bytes must not be null");
        }
        if (len < 0 || offset < 0 || offset > bytes.length - len) {
            throw new IndexOutOfBoundsException();
        }

        ensureSize(len);
        this.len = len;
        this.hashCode = 0;

        final char[] chars = this.value;

        for (int i = 0, limit = offset + len; offset < limit; offset++, i++) {
            chars[i] = (char) (bytes[offset] & 0xff);
        }
    }
    public StringValue substring(int start) {
        return substring(start, this.len);
    }
    public StringValue substring(int start, int end) {
        return new StringValue(this, start, end - start);
    }
    public void substring(StringValue target, int start) {
        substring(target, start, this.len);
    }
    public void substring(StringValue target, int start, int end) {
        target.setValue(this, start, end - start);
    }
    public int find(CharSequence str) {
        return find(str, 0);
    }
    public int find(CharSequence str, int start) {
        final int pLen = this.len;
        final int sLen = str.length();

        if (sLen == 0) {
            throw new IllegalArgumentException("Cannot find empty string.");
        }

        int pPos = start;

        final char first = str.charAt(0);

        while (pPos < pLen) {
            if (first == this.value[pPos++]) {
                // matching first character
                final int fallBackPosition = pPos;
                int sPos = 1;
                boolean found = true;

                while (sPos < sLen) {
                    if (pPos >= pLen) {
                        // no more characters in string value
                        pPos = fallBackPosition;
                        found = false;
                        break;
                    }

                    if (str.charAt(sPos++) != this.value[pPos++]) {
                        pPos = fallBackPosition;
                        found = false;
                        break;
                    }
                }
                if (found) {
                    return fallBackPosition - 1;
                }
            }
        }
        return -1;
    }
    public boolean startsWith(CharSequence prefix, int startIndex) {
        final char[] thisChars = this.value;
        final int pLen = this.len;
        final int sLen = prefix.length();

        if ((startIndex < 0) || (startIndex > pLen - sLen)) {
            return false;
        }

        int sPos = 0;
        while (sPos < sLen) {
            if (thisChars[startIndex++] != prefix.charAt(sPos++)) {
                return false;
            }
        }
        return true;
    }
    public boolean startsWith(CharSequence prefix) {
        return startsWith(prefix, 0);
    }

    @Override
    public Appendable append(char c) {
        grow(this.len + 1);
        this.value[this.len++] = c;
        return this;
    }

    @Override
    public Appendable append(CharSequence csq) {
        append(csq, 0, csq.length());
        return this;
    }
    @Override
    public Appendable append(CharSequence csq, int start, int end) {
        final int otherLen = end - start;
        grow(this.len + otherLen);
        for (int pos = start; pos < end; pos++) {
            this.value[this.len + pos] = csq.charAt(pos);
        }
        this.len += otherLen;
        return this;
    }
    public Appendable append(StringValue csq) {
        append(csq, 0, csq.length());
        return this;
    }

    public Appendable append(StringValue csq, int start, int end) {
        final int otherLen = end - start;
        grow(this.len + otherLen);
        System.arraycopy(csq.value, start, this.value, this.len, otherLen);
        this.len += otherLen;
        return this;
    }
    @Override
    public void read(final DataInputView in) throws IOException {
        int len = in.readUnsignedByte();

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            len |= curr << shift;
        }

        this.len = len;
        this.hashCode = 0;
        ensureSize(len);
        final char[] data = this.value;

        for (int i = 0; i < len; i++) {
            int c = in.readUnsignedByte();
            if (c < HIGH_BIT) {
                data[i] = (char) c;
            } else {
                int shift = 7;
                int curr;
                c = c & 0x7f;
                while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                    c |= (curr & 0x7f) << shift;
                    shift += 7;
                }
                c |= curr << shift;
                data[i] = (char) c;
            }
        }
    }

    @Override
    public void write(final DataOutputView out) throws IOException {
        int len = this.len;

        // write the length, variable-length encoded
        while (len >= HIGH_BIT) {
            out.write(len | HIGH_BIT);
            len >>>= 7;
        }
        out.write(len);

        // write the char data, variable length encoded
        for (int i = 0; i < this.len; i++) {
            int c = this.value[i];

            while (c >= HIGH_BIT) {
                out.write(c | HIGH_BIT);
                c >>>= 7;
            }
            out.write(c);
        }
    }

    @Override
    public String toString() {
        return new String(this.value, 0, this.len);
    }

    @Override
    public int compareTo(StringValue other) {
        int len1 = this.len;
        int len2 = other.len;
        int n = Math.min(len1, len2);
        char[] v1 = value;
        char[] v2 = other.value;

        for (int k = 0; k < n; k++) {
            char c1 = v1[k];
            char c2 = v2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
        }
        return len1 - len2;
    }

    @Override
    public int hashCode() {
        int h = this.hashCode;
        if (h == 0 && this.len > 0) {
            int off = 0;
            char[] val = this.value;
            int len = this.len;
            for (int i = 0; i < len; i++) {
                h = 31 * h + val[off++];
            }
            this.hashCode = h;
        }
        return h;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof StringValue) {
            final StringValue other = (StringValue) obj;
            int len = this.len;

            if (len == other.len) {
                final char[] tc = this.value;
                final char[] oc = other.value;
                int i = 0, j = 0;

                while (len-- != 0) {
                    if (tc[i++] != oc[j++]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int length() {
        return this.len;
    }

    @Override
    public char charAt(int index) {
        if (index < len) {
            return this.value[index];
        }
        else {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new StringValue(this, start, end - start);
    }

    // --------------------------------------------------------------------------------------------
    //                                   Normalized Key
    // --------------------------------------------------------------------------------------------

    @Override
    public int getMaxNormalizedKeyLen() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void copyNormalizedKey(MemorySegment target, int offset, int len) {
        // cache variables on stack, avoid repeated dereferencing of "this"
        final char[] chars = this.value;
        final int limit = offset + len;
        final int end = this.len;
        int pos = 0;

        while (pos < end && offset < limit) {
            char c = chars[pos++];
            if (c < HIGH_BIT) {
                target.put(offset++, (byte) c);
            }
            else if (c < HIGH_BIT2) {
                target.put(offset++, (byte) ((c >>> 7) | HIGH_BIT));
                if (offset < limit) {
                    target.put(offset++, (byte) c);
                }
            }
            else {
                target.put(offset++, (byte) ((c >>> 10) | HIGH_BIT2_MASK));
                if (offset < limit) {
                    target.put(offset++, (byte) (c >>> 2));
                }
                if (offset < limit) {
                    target.put(offset++, (byte) c);
                }
            }
        }
        while (offset < limit) {
            target.put(offset++, (byte) 0);
        }
    }
    // --------------------------------------------------------------------------------------------

    @Override
    public int getBinaryLength() {
        return -1;
    }

    @Override
    public void copyTo(StringValue target) {
        target.len = this.len;
        target.hashCode = this.hashCode;
        target.ensureSize(this.len);
        System.arraycopy(this.value, 0, target.value, 0, this.len);
    }

    @Override
    public StringValue copy() {
        return new StringValue(this);
    }

    @Override
    public void copy(DataInputView in, DataOutputView target) throws IOException {
        int len = in.readUnsignedByte();
        target.writeByte(len);

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                len |= (curr & 0x7f) << shift;
                shift += 7;
                target.writeByte(curr);
            }
            len |= curr << shift;
            target.writeByte(curr);
        }

        for (int i = 0; i < len; i++) {
            int c = in.readUnsignedByte();
            target.writeByte(c);
            while (c >= HIGH_BIT) {
                c = in.readUnsignedByte();
                target.writeByte(c);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //                                      Utilities
    // --------------------------------------------------------------------------------------------

    private void ensureSize(int size) {
        if (this.value.length < size) {
            this.value = new char[size];
        }
    }

    /**
     * Grow and retain content.
     */
    private void grow(int size) {
        if (this.value.length < size) {
            char[] value = new char[ Math.max(this.value.length * 3 / 2, size)];
            System.arraycopy(this.value, 0, value, 0, this.len);
            this.value = value;
        }
    }

    // --------------------------------------------------------------------------------------------
    //                           Static Helpers for String Serialization
    // --------------------------------------------------------------------------------------------

    public static String readString(DataInput in) throws IOException {
        // the length we read is offset by one, because a length of zero indicates a null value
        int len = in.readUnsignedByte();

        if (len == 0) {
            return null;
        }

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            len |= curr << shift;
        }

        // subtract one for the null length
        len -= 1;

        final char[] data = new char[len];

        for (int i = 0; i < len; i++) {
            int c = in.readUnsignedByte();
            if (c < HIGH_BIT) {
                data[i] = (char) c;
            } else {
                int shift = 7;
                int curr;
                c = c & 0x7f;
                while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                    c |= (curr & 0x7f) << shift;
                    shift += 7;
                }
                c |= curr << shift;
                data[i] = (char) c;
            }
        }

        return new String(data, 0, len);
    }

    public static final void writeString(CharSequence cs, DataOutput out) throws IOException {
        if (cs != null) {
            // the length we write is offset by one, because a length of zero indicates a null value
            int lenToWrite = cs.length()+1;
            if (lenToWrite < 0) {
                throw new IllegalArgumentException("CharSequence is too long.");
            }

            // write the length, variable-length encoded
            while (lenToWrite >= HIGH_BIT) {
                out.write(lenToWrite | HIGH_BIT);
                lenToWrite >>>= 7;
            }
            out.write(lenToWrite);

            // write the char data, variable length encoded
            for (int i = 0; i < cs.length(); i++) {
                int c = cs.charAt(i);

                while (c >= HIGH_BIT) {
                    out.write(c | HIGH_BIT);
                    c >>>= 7;
                }
                out.write(c);
            }
        } else {
            out.write(0);
        }
    }

    public static final void copyString(DataInput in, DataOutput out) throws IOException {
        int len = in.readUnsignedByte();
        out.writeByte(len);

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = in.readUnsignedByte()) >= HIGH_BIT) {
                out.writeByte(curr);
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            out.writeByte(curr);
            len |= curr << shift;
        }

        // note that the length is one larger than the actual length (length 0 is a null string, not a zero length string)
        len--;

        for (int i = 0; i < len; i++) {
            int c = in.readUnsignedByte();
            out.writeByte(c);
            while (c >= HIGH_BIT) {
                c = in.readUnsignedByte();
                out.writeByte(c);
            }
        }
    }


}
