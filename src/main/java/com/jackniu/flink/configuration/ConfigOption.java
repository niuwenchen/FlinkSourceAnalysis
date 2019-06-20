package com.jackniu.flink.configuration;

import com.jackniu.flink.configuration.description.Description;

import java.util.Arrays;
import java.util.Collections;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/6/6.
 *
 * describes a configuration parameter. It encapsulates
 * the configuration key, deprecated older versions of the key, and an optional
 * default value for the configuration parameter.
 */
public class ConfigOption<T> {
    private static final String[] EMPTY = new String[0];

    /** The current key for that config option. */
    private final String key;

    /** The list of deprecated keys, in the order to be checked. */
    private final String[] deprecatedKeys;

    /** The default value for this option. */
    private final T defaultValue;

    /** The description for this option. */
    private final Description description;

    // Creates a new config option with no deprecated keys.
    ConfigOption(String key, T defaultValue){
        this.key = checkNotNull(key);
        this.description = Description.builder().text("").build();
        this.defaultValue = defaultValue;
        this.deprecatedKeys = EMPTY;
    }


    @Deprecated
    ConfigOption(String key, String description, T defaultValue, String... deprecatedKeys) {
        this.key = checkNotNull(key);
        this.description = Description.builder().text(description).build();
        this.defaultValue = defaultValue;
        this.deprecatedKeys = deprecatedKeys == null || deprecatedKeys.length == 0 ? EMPTY : deprecatedKeys;
    }

    ConfigOption(String key, Description description, T defaultValue, String... deprecatedKeys) {
        this.key = checkNotNull(key);
        this.description = description;
        this.defaultValue = defaultValue;
        this.deprecatedKeys = deprecatedKeys == null || deprecatedKeys.length == 0 ? EMPTY : deprecatedKeys;
    }

    public ConfigOption<T> withDeprecatedKeys(String... deprecatedKeys) {
        return new ConfigOption<>(key, description, defaultValue, deprecatedKeys);
    }

    //使用此选项的键和默认值创建一个新的配置选项，并添加给定的描述。生成配置文档时使用给定的描述。
    public ConfigOption<T> withDescription(final String description) {
        return withDescription(Description.builder().text(description).build());
    }

    public ConfigOption<T> withDescription(final Description description) {
        return new ConfigOption<>(key, description, defaultValue, deprecatedKeys);
    }

    public String key(){
        return key;
    }

    public boolean hasDefaultValue(){
        return defaultValue !=null;
    }

    public T defaultValue() {
        return defaultValue;
    }


    public boolean hasDeprecatedKeys() {
        return deprecatedKeys != EMPTY;
    }

    public Iterable<String> deprecatedKeys() {
        return deprecatedKeys == EMPTY ? Collections.<String>emptyList() : Arrays.asList(deprecatedKeys);
    }

    public Description description() {
        return description;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else if (o != null && o.getClass() == ConfigOption.class) {
            ConfigOption<?> that = (ConfigOption<?>) o;
            return this.key.equals(that.key) &&
                    Arrays.equals(this.deprecatedKeys, that.deprecatedKeys) &&
                    (this.defaultValue == null ? that.defaultValue == null :
                            (that.defaultValue != null && this.defaultValue.equals(that.defaultValue)));
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() +
                17 * Arrays.hashCode(deprecatedKeys) +
                (defaultValue != null ? defaultValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return String.format("Key: '%s' , default: %s (deprecated keys: %s)",
                key, defaultValue, Arrays.toString(deprecatedKeys));
    }



}
