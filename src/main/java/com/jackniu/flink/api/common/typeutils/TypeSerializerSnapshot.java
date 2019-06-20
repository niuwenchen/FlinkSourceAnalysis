package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.annotations.PublicEvolving;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/18.
 */
@PublicEvolving
public interface  TypeSerializerSnapshot<T> {
    int getCurrentVersion();

    void writeSnapshot(DataOutputView out) throws IOException;

    void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;

    TypeSerializer<T> restoreSerializer();

    TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer);

    static void writeVersionedSnapshot(DataOutputView out, TypeSerializerSnapshot<?> snapshot) throws IOException {
        out.writeUTF(snapshot.getClass().getName());
        out.writeInt(snapshot.getCurrentVersion());
        snapshot.writeSnapshot(out);
    }

    static <T> TypeSerializerSnapshot<T> readVersionedSnapshot(DataInputView in, ClassLoader cl) throws IOException {
        final TypeSerializerSnapshot<T> snapshot =
                TypeSerializerSnapshotSerializationUtil.readAndInstantiateSnapshotClass(in, cl);

        final int version = in.readInt();
        snapshot.readSnapshot(version, in, cl);

        return snapshot;
    }
}
