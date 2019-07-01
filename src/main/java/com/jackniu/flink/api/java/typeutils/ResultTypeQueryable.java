package com.jackniu.flink.api.java.typeutils;

import com.jackniu.flink.api.common.typeinfo.TypeInformation;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface ResultTypeQueryable<T> {
    TypeInformation<T> getProducedType();
}
