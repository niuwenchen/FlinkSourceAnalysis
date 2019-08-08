package com.jackniu.flink.metrics;

import java.util.Map;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface MetricGroup {
    Counter counter(int name);
    Counter counter(String name);
    <C extends Counter> C counter(int name, C counter);
    <C extends Counter> C counter(String name, C counter);
    <T, G extends Gauge<T>> G gauge(int name, G gauge);
    <T, G extends Gauge<T>> G gauge(String name, G gauge);
    <H extends Histogram> H histogram(String name, H histogram);

    <H extends Histogram> H histogram(int name, H histogram);
    <M extends Meter> M meter(String name, M meter);
    <M extends Meter> M meter(int name, M meter);
    MetricGroup addGroup(int name);
    MetricGroup addGroup(String name);
    MetricGroup addGroup(String key, String value);
    String[] getScopeComponents();
    Map<String, String> getAllVariables();
    String getMetricIdentifier(String metricName);
    String getMetricIdentifier(String metricName, CharacterFilter filter);

}
