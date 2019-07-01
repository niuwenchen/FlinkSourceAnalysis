package com.jackniu.flink.api.java.typeutils.runtime.kyro;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.ObjectMap;
import com.jackniu.flink.util.InstantiationUtil;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by JackNiu on 2019/6/27.
 */
public class JavaSerializer<T> extends Serializer<T> {
    public JavaSerializer() {}

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void write(Kryo kryo, Output output, T o) {
        try {
            ObjectMap graphContext = kryo.getGraphContext();
            ObjectOutputStream objectStream = (ObjectOutputStream)graphContext.get(this);
            if (objectStream == null) {
                objectStream = new ObjectOutputStream(output);
                graphContext.put(this, objectStream);
            }
            objectStream.writeObject(o);
            objectStream.flush();
        } catch (Exception ex) {
            throw new KryoException("Error during Java serialization.", ex);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public T read(Kryo kryo, Input input, Class aClass) {
        try {
            ObjectMap graphContext = kryo.getGraphContext();
            ObjectInputStream objectStream = (ObjectInputStream)graphContext.get(this);
            if (objectStream == null) {
                // make sure we use Kryo's classloader
                objectStream = new InstantiationUtil.ClassLoaderObjectInputStream(input, kryo.getClassLoader());
                graphContext.put(this, objectStream);
            }
            return (T) objectStream.readObject();
        } catch (Exception ex) {
            throw new KryoException("Error during Java deserialization.", ex);
        }
    }
}
