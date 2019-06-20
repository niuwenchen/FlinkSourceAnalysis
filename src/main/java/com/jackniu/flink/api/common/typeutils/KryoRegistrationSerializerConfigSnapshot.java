package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.core.memory.DataOutputView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by JackNiu on 2019/6/20.
 */
public abstract class KryoRegistrationSerializerConfigSnapshot<T> extends GenericTypeSerializerConfigSnapshot<T>  {

    public static class DummyRegisteredClass implements Serializable {}

}
