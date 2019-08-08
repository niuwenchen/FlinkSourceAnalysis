package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.configuration.Configuration;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface TypeSerializerFactory<T> {
    void writeParametersToConfig(Configuration config);

    void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException;

    TypeSerializer<T> getSerializer();

    Class<T> getDataType();
}
