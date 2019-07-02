package com.jackniu.flink.configuration;

/**
 * Created by JackNiu on 2019/6/10.
 */
public class Test {
    public static void main(String[] args) {
        ConfigOption<String> name = ConfigOptions.key("name").defaultValue("JackNiu");
        ConfigOption<String> name1 = new ConfigOption<String>("name","JackNiu");
        System.out.println(name);
        System.out.println(name1);
        System.out.println(name.equals(name1));
//        System.out.println(name.);
    }
}
