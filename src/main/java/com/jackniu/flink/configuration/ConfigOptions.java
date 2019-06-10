package com.jackniu.flink.configuration;

/**
 * Created by JackNiu on 2019/6/10.
 */

/**
 * 该类用来建立ConfigOption
 * 通常采用以下模式之一构建:
 * 具有默认值的简单字符串值选项
 *  ConfigOption<String> tempDirs = ConfigOptions.key("tmp.dir").defaultValue("/tmp");
 *  ConfigOption<Integer> parallelism = ConfigOptions.key("application.parallelism").defaultValue(100);
 *  ConfigOption<String> userName = ConfigOptions.key("uer.name").noDefaultValue();
 *  ConfigOption<Double> threshold = ConfigOptions.key("cpu.utilization.threshold").defaultValue(0.9).withDeprecatedKeys("cpu.threshold");
 */
import static com.jackniu.flink.util.Preconditions.checkNotNull;
public class ConfigOptions {

    public static OptionBuilder key(String key) {
        checkNotNull(key);
        return new OptionBuilder(key);
    }


    public static final class OptionBuilder {
        private final String key;

        //Creates a new OptionBuilder.
        OptionBuilder(String key){
            this.key = key;
        }

        public <T> ConfigOption<T> defaultValue(T value){
            checkNotNull(value);
            return  new ConfigOption<T>(key,value);
        }
        public ConfigOption<String> noDefaultValue() {
            return new ConfigOption<>(key, null);
        }
    }

    //不打算实例化
    private ConfigOptions() {}

}
