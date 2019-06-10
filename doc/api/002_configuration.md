## com.jackniu.flink.configuration
### ConfigOptions
    配置选项，该类用来建立ConfigOption
    *  ConfigOption<String> tempDirs = ConfigOptions.key("tmp.dir").defaultValue("/tmp");
    *  ConfigOption<Integer> parallelism = ConfigOptions.key("application.parallelism").defaultValue(100);
    *  ConfigOption<String> userName = ConfigOptions.key("uer.name").noDefaultValue();
    *  ConfigOption<Double> threshold = ConfigOptions.key("cpu.utilization.threshold").defaultValue(0.9).withDeprecatedKeys("cpu.threshold");
    其实就是通过该类的代码获取一个默认的值，然后改值用另一个需要使用的地方包装
    就像上面的使用方式那样

### ConfigOption
描述配置参数。它封装了配置键、废弃的旧版本的键以及配置参数的可选默认值。

    //Creates a new config option with no deprecated keys. 
    ConfigOption(String key, T defaultValue){
            this.key = checkNotNull(key);
            this.description = Description.builder().text("").build();
            this.defaultValue = defaultValue;
            this.deprecatedKeys = EMPTY;
        }
     
    
    public ConfigOption<T> withDescription(final String description) {
            return withDescription(Description.builder().text(description).build());
        }
        
    
    运行
    ConfigOption<String> name = ConfigOptions.key("name").defaultValue("JackNiu");
            ConfigOption<String> name1 = new ConfigOption<String>("name","JackNiu");
            System.out.println(name);
            System.out.println(name1);
            System.out.println(name.equals(name1));
    Key: 'name' , default: JackNiu (deprecated keys: [])
    Key: 'name' , default: JackNiu (deprecated keys: [])
    true
    


## MetricOptions
可选的记者姓名列表。如果配置成功，将只启动名称与列表中任何名称匹配的记者。否则，将启动在配置中可以找到的所有记者。

    * examples
     *  metrics.reporters = foo,bar
     *  metrics.reporter.foo.class = org.apache.flink.metrics.reporter.JMXReporter
     *  metrics.reporter.foo.interval = 10
     *
     *  metrics.reporter.bar.class = org.apache.flink.metrics.graphite.GraphiteReporter
     *  metrics.reporter.bar.port = 1337


    