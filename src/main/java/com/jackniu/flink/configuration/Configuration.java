package com.jackniu.flink.configuration;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.api.common.ExecutionConfig;
import com.jackniu.flink.core.io.IOReadableWritable;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.types.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by JackNiu on 2019/6/19.
 */
public class Configuration  extends ExecutionConfig.GlobalJobParameters
        implements IOReadableWritable, java.io.Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private static final byte TYPE_STRING = 0;
    private static final byte TYPE_INT = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_BOOLEAN = 3;
    private static final byte TYPE_FLOAT = 4;
    private static final byte TYPE_DOUBLE = 5;
    private static final byte TYPE_BYTES = 6;

    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);


    /** Stores the concrete key/value pairs of this configuration object. */
    protected final HashMap<String, Object> confData;

    public Configuration() { this.confData = new HashMap<>();}
    public Configuration(Configuration other) {
        this.confData = new HashMap<>(other.confData);
    }

    //Returns the class associated with the given key as a string.
    public <T> Class<T> getClass(String key,Class<? extends T> defaultValue,ClassLoader classLoader) throws ClassNotFoundException{
        Object o = getRawValue(key);
        if (o == null){
            return (Class<T>) defaultValue;
        }

        if (o.getClass() == String.class){
            return (Class<T>) Class.forName((String) o,true,classLoader);
        }
        LOG.warn("Configuration cannot evaluate value " + o + " as a class name");
        return (Class<T>) defaultValue;

    }

    public void setClass(String key, Class<?> klazz) {
        setValueInternal(key, klazz.getName());
    }

    public String getString(String key, String defaultValue) {
        Object o = getRawValue(key);
        if (o == null) {
            return defaultValue;
        } else {
            return o.toString();
        }
    }
    @PublicEvolving
    public String getString(ConfigOption<String> configOption) {
        Object o = getValueOrDefaultFromOption(configOption);
        return o == null ? null : o.toString();
    }
    @PublicEvolving
    public String getString(ConfigOption<String> configOption, String overrideDefault) {
        Object o = getRawValueFromOption(configOption);
        return o == null ? overrideDefault : o.toString();
    }

    public void setString(String key, String value) {
        setValueInternal(key, value);
    }
    @PublicEvolving
    public void setString(ConfigOption<String> key, String value) {
        setValueInternal(key.key(), value);
    }

    public int getInteger(String key, int defaultValue) {
        Object o = getRawValue(key);
        if (o == null) {
            return defaultValue;
        }

        return convertToInt(o, defaultValue);
    }

    @PublicEvolving
    public int getInteger(ConfigOption<Integer> configOption) {
        Object o = getValueOrDefaultFromOption(configOption);
        return convertToInt(o, configOption.defaultValue());
    }

    @PublicEvolving
    public int getInteger(ConfigOption<Integer> configOption, int overrideDefault) {
        Object o = getRawValueFromOption(configOption);
        if (o == null) {
            return overrideDefault;
        }
        return convertToInt(o, configOption.defaultValue());
    }
    public void setInteger(String key, int value) {
        setValueInternal(key, value);
    }
    @PublicEvolving
    public void setInteger(ConfigOption<Integer> key, int value) {
        setValueInternal(key.key(), value);
    }

    public long getLong(String key, long defaultValue) {
        Object o = getRawValue(key);
        if (o == null) {
            return defaultValue;
        }

        return convertToLong(o, defaultValue);
    }

    @PublicEvolving
    public long getLong(ConfigOption<Long> configOption) {
        Object o = getValueOrDefaultFromOption(configOption);
        return convertToLong(o, configOption.defaultValue());
    }
    @PublicEvolving
    public long getLong(ConfigOption<Long> configOption, long overrideDefault) {
        Object o = getRawValueFromOption(configOption);
        if (o == null) {
            return overrideDefault;
        }
        return convertToLong(o, configOption.defaultValue());
    }

    public void setLong(String key, long value) {
        setValueInternal(key, value);
    }
    public void setLong(ConfigOption<Long> key, long value) {
        setValueInternal(key.key(), value);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        Object o = getRawValue(key);
        if (o == null) {
            return defaultValue;
        }

        return convertToBoolean(o);
    }

    @PublicEvolving
    public boolean getBoolean(ConfigOption<Boolean> configOption) {
        Object o = getValueOrDefaultFromOption(configOption);
        return convertToBoolean(o);
    }
    @PublicEvolving
    public boolean getBoolean(ConfigOption<Boolean> configOption, boolean overrideDefault) {
        Object o = getRawValueFromOption(configOption);
        if (o == null) {
            return overrideDefault;
        }
        return convertToBoolean(o);
    }

    public void setBoolean(String key, boolean value) {
        setValueInternal(key, value);
    }
    public void setBoolean(ConfigOption<Boolean> key, boolean value) {
        setValueInternal(key.key(), value);
    }

    public float getFloat(String key, float defaultValue) {
        Object o = getRawValue(key);
        if (o == null) {
            return defaultValue;
        }

        return convertToFloat(o, defaultValue);
    }
    @PublicEvolving
    public float getFloat(ConfigOption<Float> configOption) {
        Object o = getValueOrDefaultFromOption(configOption);
        return convertToFloat(o, configOption.defaultValue());
    }
    @PublicEvolving
    public float getFloat(ConfigOption<Float> configOption, float overrideDefault) {
        Object o = getRawValueFromOption(configOption);
        if (o == null) {
            return overrideDefault;
        }
        return convertToFloat(o, configOption.defaultValue());
    }
    public void setFloat(String key, float value) {
        setValueInternal(key, value);
    }
    @PublicEvolving
    public void setFloat(ConfigOption<Float> key, float value) {
        setValueInternal(key.key(), value);
    }

    public double getDouble(String key, double defaultValue) {
        Object o = getRawValue(key);
        if (o == null) {
            return defaultValue;
        }

        return convertToDouble(o, defaultValue);
    }
    @PublicEvolving
    public double getDouble(ConfigOption<Double> configOption) {
        Object o = getValueOrDefaultFromOption(configOption);
        return convertToDouble(o, configOption.defaultValue());
    }
    @PublicEvolving
    public double getDouble(ConfigOption<Double> configOption, double overrideDefault) {
        Object o = getRawValueFromOption(configOption);
        if (o == null) {
            return overrideDefault;
        }
        return convertToDouble(o, configOption.defaultValue());
    }
    public void setDouble(String key, double value) {
        setValueInternal(key, value);
    }

    @PublicEvolving
    public void setDouble(ConfigOption<Double> key, double value) {
        setValueInternal(key.key(), value);
    }

    @SuppressWarnings("EqualsBetweenInconvertibleTypes")
    public byte[] getBytes(String key, byte[] defaultValue) {

        Object o = getRawValue(key);
        if (o == null) {
            return defaultValue;
        }
        else if (o.getClass().equals(byte[].class)) {
            return (byte[]) o;
        }
        else {
            LOG.warn("Configuration cannot evaluate value {} as a byte[] value", o);
            return defaultValue;
        }
    }
    public void setBytes(String key, byte[] bytes) {
        setValueInternal(key, bytes);
    }
    @PublicEvolving
    public String getValue(ConfigOption<?> configOption) {
        Object o = getValueOrDefaultFromOption(configOption);
        return o == null ? null : o.toString();
    }
    public Set<String> keySet() {
        synchronized (this.confData) {
            return new HashSet<>(this.confData.keySet());
        }
    }

    public void addAllToProperties(Properties props) {
        synchronized (this.confData) {
            for (Map.Entry<String, Object> entry : this.confData.entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void addAll(Configuration other) {
        synchronized (this.confData) {
            synchronized (other.confData) {
                this.confData.putAll(other.confData);
            }
        }
    }

    public void addAll(Configuration other, String prefix) {
        final StringBuilder bld = new StringBuilder();
        bld.append(prefix);
        final int pl = bld.length();

        synchronized (this.confData) {
            synchronized (other.confData) {
                for (Map.Entry<String, Object> entry : other.confData.entrySet()) {
                    bld.setLength(pl);
                    bld.append(entry.getKey());
                    this.confData.put(bld.toString(), entry.getValue());
                }
            }
        }
    }


    @Override
    public Configuration clone() {
        Configuration config = new Configuration();
        config.addAll(this);

        return config;
    }

    public boolean containsKey(String key){
        synchronized (this.confData){
            return this.confData.containsKey(key);
        }
    }
    @PublicEvolving
    public boolean contains(ConfigOption<?> configOption) {
        synchronized (this.confData){
            // first try the current key
            if (this.confData.containsKey(configOption.key())) {
                return true;
            }
            else if (configOption.hasDeprecatedKeys()) {
                // try the deprecated keys
                for (String deprecatedKey : configOption.deprecatedKeys()) {
                    if (this.confData.containsKey(deprecatedKey)) {
                        LOG.warn("Config uses deprecated configuration key '{}' instead of proper key '{}'",
                                deprecatedKey, configOption.key());
                        return true;
                    }
                }
            }

            return false;
        }
    }
    @Override
    public Map<String, String> toMap() {
        synchronized (this.confData){
            Map<String, String> ret = new HashMap<>(this.confData.size());
            for (Map.Entry<String, Object> entry : confData.entrySet()) {
                ret.put(entry.getKey(), entry.getValue().toString());
            }
            return ret;
        }
    }

    public <T> boolean removeConfig(ConfigOption<T> configOption){
        synchronized (this.confData){
            // try the current key
            Object oldValue = this.confData.remove(configOption.key());
            if (oldValue == null){
                for (String deprecatedKey : configOption.deprecatedKeys()){
                    oldValue = this.confData.remove(deprecatedKey);
                    if (oldValue != null){
                        LOG.warn("Config uses deprecated configuration key '{}' instead of proper key '{}'",
                                deprecatedKey, configOption.key());
                        return true;
                    }
                }
                return false;
            }
            return true;
        }
    }



    private Object getValueOrDefaultFromOption(ConfigOption<?> configOption) {
        Object o = getRawValueFromOption(configOption);
        return o != null ? o : configOption.defaultValue();
    }

    private Object getRawValueFromOption(ConfigOption<?> configOption) {
        // first try the current key
        Object o = getRawValue(configOption.key());

        if (o != null) {
            // found a value for the current proper key
            return o;
        }
        else if (configOption.hasDeprecatedKeys()) {
            // try the deprecated keys
            for (String deprecatedKey : configOption.deprecatedKeys()) {
                Object oo = getRawValue(deprecatedKey);
                if (oo != null) {
                    LOG.warn("Config uses deprecated configuration key '{}' instead of proper key '{}'",
                            deprecatedKey, configOption.key());
                    return oo;
                }
            }
        }

        return null;
    }

    private Object getRawValue(String key){
        if (key ==  null){
            throw new NullPointerException("Key must not be null....ok");
        }
        synchronized (this.confData){
            return this.confData.get(key);
        }
    }

    <T> void setValueInternal(String key,T value){
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        if (value == null) {
            throw new NullPointerException("Value must not be null.");
        }

        synchronized (this.confData) {
            this.confData.put(key, value);
        }
    }

    private int convertToInt(Object o, int defaultValue) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        }
        else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                LOG.warn("Configuration value {} overflows/underflows the integer type.", value);
                return defaultValue;
            }
        }
        else {
            try {
                return Integer.parseInt(o.toString());
            }
            catch (NumberFormatException e) {
                LOG.warn("Configuration cannot evaluate value {} as an integer number", o);
                return defaultValue;
            }
        }
    }

    private long convertToLong(Object o, long defaultValue) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        }
        else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }
        else {
            try {
                return Long.parseLong(o.toString());
            }
            catch (NumberFormatException e) {
                LOG.warn("Configuration cannot evaluate value " + o + " as a long integer number");
                return defaultValue;
            }
        }
    }

    private boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }
        else {
            return Boolean.parseBoolean(o.toString());
        }
    }

    private float convertToFloat(Object o, float defaultValue) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        }
        else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                LOG.warn("Configuration value {} overflows/underflows the float type.", value);
                return defaultValue;
            }
        }
        else {
            try {
                return Float.parseFloat(o.toString());
            }
            catch (NumberFormatException e) {
                LOG.warn("Configuration cannot evaluate value {} as a float value", o);
                return defaultValue;
            }
        }
    }

    private double convertToDouble(Object o, double defaultValue) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        }
        else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }
        else {
            try {
                return Double.parseDouble(o.toString());
            }
            catch (NumberFormatException e) {
                LOG.warn("Configuration cannot evaluate value {} as a double value", o);
                return defaultValue;
            }
        }
    }


    @Override
    public void read(DataInputView in) throws IOException {
        synchronized (this.confData) {
            final int numberOfProperties = in.readInt();

            for (int i = 0; i < numberOfProperties; i++) {
                String key = StringValue.readString(in);
                Object value;

                byte type = in.readByte();
                switch (type) {
                    case TYPE_STRING:
                        value = StringValue.readString(in);
                        break;
                    case TYPE_INT:
                        value = in.readInt();
                        break;
                    case TYPE_LONG:
                        value = in.readLong();
                        break;
                    case TYPE_FLOAT:
                        value = in.readFloat();
                        break;
                    case TYPE_DOUBLE:
                        value = in.readDouble();
                        break;
                    case TYPE_BOOLEAN:
                        value = in.readBoolean();
                        break;
                    case TYPE_BYTES:
                        byte[] bytes = new byte[in.readInt()];
                        in.readFully(bytes);
                        value = bytes;
                        break;
                    default:
                        throw new IOException("Unrecognized type: " + type);
                }

                this.confData.put(key, value);
            }
        }
    }

    @Override
    public void write(final DataOutputView out) throws IOException {
        synchronized (this.confData) {
            out.writeInt(this.confData.size());

            for (Map.Entry<String, Object> entry : this.confData.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();

                StringValue.writeString(key, out);
                Class<?> clazz = val.getClass();

                if (clazz == String.class) {
                    out.write(TYPE_STRING);
                    StringValue.writeString((String) val, out);
                }
                else if (clazz == Integer.class) {
                    out.write(TYPE_INT);
                    out.writeInt((Integer) val);
                }
                else if (clazz == Long.class) {
                    out.write(TYPE_LONG);
                    out.writeLong((Long) val);
                }
                else if (clazz == Float.class) {
                    out.write(TYPE_FLOAT);
                    out.writeFloat((Float) val);
                }
                else if (clazz == Double.class) {
                    out.write(TYPE_DOUBLE);
                    out.writeDouble((Double) val);
                }
                else if (clazz == byte[].class) {
                    out.write(TYPE_BYTES);
                    byte[] bytes = (byte[]) val;
                    out.writeInt(bytes.length);
                    out.write(bytes);
                }
                else if (clazz == Boolean.class) {
                    out.write(TYPE_BOOLEAN);
                    out.writeBoolean((Boolean) val);
                }
                else {
                    throw new IllegalArgumentException("Unrecognized type");
                }
            }
        }
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (String s : this.confData.keySet()) {
            hash ^= s.hashCode();
        }
        return hash;
    }

    @SuppressWarnings("EqualsBetweenInconvertibleTypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        else if (obj instanceof Configuration) {
            Map<String, Object> otherConf = ((Configuration) obj).confData;

            for (Map.Entry<String, Object> e : this.confData.entrySet()) {
                Object thisVal = e.getValue();
                Object otherVal = otherConf.get(e.getKey());

                if (!thisVal.getClass().equals(byte[].class)) {
                    if (!thisVal.equals(otherVal)) {
                        return false;
                    }
                } else if (otherVal.getClass().equals(byte[].class)) {
                    if (!Arrays.equals((byte[]) thisVal, (byte[]) otherVal)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {
        return this.confData.toString();
    }

}
