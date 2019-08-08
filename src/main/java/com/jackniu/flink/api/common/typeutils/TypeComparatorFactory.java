package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.configuration.Configuration;

/**
 * Created by JackNiu on 2019/7/8.
 */
public interface TypeComparatorFactory<T> {
    void writeParametersToConfig(Configuration config);

    void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException;

    TypeComparator<T> createComparator();
}
