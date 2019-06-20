package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.api.java.tuple.Tuple2;
import com.jackniu.flink.core.io.VersionedIOReadableWritable;
import com.jackniu.flink.core.memory.*;
import com.jackniu.flink.util.InstantiationUtil;
import com.jackniu.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JackNiu on 2019/6/19.
 */
public class TypeSerializerSerializationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TypeSerializerSerializationUtil.class);

    public static <T> void writeSerializer(DataOutputView out, TypeSerializer<T> serializer) throws IOException {
        new TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<>(serializer).write(out);
    }
    public static <T> TypeSerializer<T> tryReadSerializer(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        return tryReadSerializer(in, userCodeClassLoader, false);
    }

    public static <T> TypeSerializer<T> tryReadSerializer(
            DataInputView in,
            ClassLoader userCodeClassLoader,
            boolean useDummyPlaceholder) throws IOException {

        final TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<T> proxy =
                new TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<>(userCodeClassLoader);

        try {
            proxy.read(in);
            return proxy.getTypeSerializer();
        } catch (UnloadableTypeSerializerException e) {
            if (useDummyPlaceholder) {
                LOG.warn("Could not read a requested serializer. Replaced with a UnloadableDummyTypeSerializer.", e.getCause());
                return new UnloadableDummyTypeSerializer<>(e.getSerializerBytes(), e.getCause());
            } else {
                throw e;
            }
        }
    }

    public static void writeSerializersAndConfigsWithResilience(
            DataOutputView out,
            List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializersAndConfigs) throws IOException {

        try (
                ByteArrayOutputStreamWithPos bufferWithPos = new ByteArrayOutputStreamWithPos();
                DataOutputViewStreamWrapper bufferWrapper = new DataOutputViewStreamWrapper(bufferWithPos)) {

            out.writeInt(serializersAndConfigs.size());
            for (Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>> serAndConfSnapshot : serializersAndConfigs) {
                out.writeInt(bufferWithPos.getPosition());
                writeSerializer(bufferWrapper, serAndConfSnapshot.f0);

                out.writeInt(bufferWithPos.getPosition());
                TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                        bufferWrapper, (TypeSerializerSnapshot) serAndConfSnapshot.f1, serAndConfSnapshot.f0);
            }

            out.writeInt(bufferWithPos.getPosition());
            out.write(bufferWithPos.getBuf(), 0, bufferWithPos.getPosition());
        }
    }


    public static List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> readSerializersAndConfigsWithResilience(
            DataInputView in,
            ClassLoader userCodeClassLoader) throws IOException {

        int numSerializersAndConfigSnapshots = in.readInt();

        int[] offsets = new int[numSerializersAndConfigSnapshots * 2];

        for (int i = 0; i < numSerializersAndConfigSnapshots; i++) {
            offsets[i * 2] = in.readInt();
            offsets[i * 2 + 1] = in.readInt();
        }

        int totalBytes = in.readInt();
        byte[] buffer = new byte[totalBytes];
        in.readFully(buffer);

        List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializersAndConfigSnapshots =
                new ArrayList<>(numSerializersAndConfigSnapshots);

        TypeSerializer<?> serializer;
        TypeSerializerSnapshot<?> configSnapshot;
        try (
                ByteArrayInputStreamWithPos bufferWithPos = new ByteArrayInputStreamWithPos(buffer);
                DataInputViewStreamWrapper bufferWrapper = new DataInputViewStreamWrapper(bufferWithPos)) {

            for (int i = 0; i < numSerializersAndConfigSnapshots; i++) {

                bufferWithPos.setPosition(offsets[i * 2]);
                serializer = tryReadSerializer(bufferWrapper, userCodeClassLoader, true);

                bufferWithPos.setPosition(offsets[i * 2 + 1]);

                configSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                        bufferWrapper, userCodeClassLoader, serializer);

                serializersAndConfigSnapshots.add(new Tuple2<>(serializer, configSnapshot));
            }
        }

        return serializersAndConfigSnapshots;
    }


    /**
     * Utility serialization proxy for a {@link TypeSerializer}.
     */
    public static final class TypeSerializerSerializationProxy<T> extends VersionedIOReadableWritable {

        private static final int VERSION = 1;

        private ClassLoader userClassLoader;
        private TypeSerializer<T> typeSerializer;

        public TypeSerializerSerializationProxy(ClassLoader userClassLoader) {
            this.userClassLoader = userClassLoader;
        }

        public TypeSerializerSerializationProxy(TypeSerializer<T> typeSerializer) {
            this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
        }

        public TypeSerializer<T> getTypeSerializer() {
            return typeSerializer;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            super.write(out);

            if (typeSerializer instanceof UnloadableDummyTypeSerializer) {
                UnloadableDummyTypeSerializer<T> dummyTypeSerializer =
                        (UnloadableDummyTypeSerializer<T>) this.typeSerializer;

                byte[] serializerBytes = dummyTypeSerializer.getActualBytes();
                out.write(serializerBytes.length);
                out.write(serializerBytes);
            } else {
                // write in a way that allows the stream to recover from exceptions
                try (ByteArrayOutputStreamWithPos streamWithPos = new ByteArrayOutputStreamWithPos()) {
                    InstantiationUtil.serializeObject(streamWithPos, typeSerializer);
                    out.writeInt(streamWithPos.getPosition());
                    out.write(streamWithPos.getBuf(), 0, streamWithPos.getPosition());
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(DataInputView in) throws IOException {
            super.read(in);

            // read in a way that allows the stream to recover from exceptions
            int serializerBytes = in.readInt();
            byte[] buffer = new byte[serializerBytes];
            in.readFully(buffer);

            ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            try (
                    InstantiationUtil.FailureTolerantObjectInputStream ois =
                            new InstantiationUtil.FailureTolerantObjectInputStream(new ByteArrayInputStream(buffer), userClassLoader)) {

                Thread.currentThread().setContextClassLoader(userClassLoader);
                typeSerializer = (TypeSerializer<T>) ois.readObject();
            } catch (Exception e) {
                throw new UnloadableTypeSerializerException(e, buffer);
            } finally {
                Thread.currentThread().setContextClassLoader(previousClassLoader);
            }
        }

        @Override
        public int getVersion() {
            return VERSION;
        }
    }

    // ------------------------------------------------------------------------
    //  utility exception
    // ------------------------------------------------------------------------

    /**
     * An exception thrown to indicate that a serializer cannot be read.
     * It wraps the cause of the read error, as well as the original bytes of the written serializer.
     */
    private static class UnloadableTypeSerializerException extends IOException {

        private static final long serialVersionUID = 1L;

        private final byte[] serializerBytes;

        /**
         * Creates a new exception, with the cause of the read error and the original serializer bytes.
         *
         * @param cause the cause of the read error.
         * @param serializerBytes the original serializer bytes.
         */
        public UnloadableTypeSerializerException(Exception cause, byte[] serializerBytes) {
            super(cause);
            this.serializerBytes = Preconditions.checkNotNull(serializerBytes);
        }

        public byte[] getSerializerBytes() {
            return serializerBytes;
        }
    }




}
