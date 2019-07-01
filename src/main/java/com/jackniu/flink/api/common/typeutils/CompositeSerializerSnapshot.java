package com.jackniu.flink.api.common.typeutils;

import com.jackniu.flink.api.java.tuple.Tuple2;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.jackniu.flink.util.Preconditions.checkArgument;

/**
 * Created by JackNiu on 2019/6/27.
 */
public class CompositeSerializerSnapshot {

    public static final int MAGIC_NUMBER=1333245;
    private static  final int VERSION=1;
    private final TypeSerializerSnapshot<?>[] nestedSnapshots;
    public CompositeSerializerSnapshot(TypeSerializer<?>... serializers) {
        this.nestedSnapshots = TypeSerializerUtils.snapshotBackwardsCompatible(serializers);
    }
    private CompositeSerializerSnapshot(TypeSerializerSnapshot<?>[] snapshots) {
        this.nestedSnapshots = snapshots;
    }
    public TypeSerializer<?>[] getRestoreSerializers() {
        return snapshotsToRestoreSerializers(nestedSnapshots);
    }
    public <T> TypeSerializer<T> getRestoreSerializer(int pos) {
        checkArgument(pos < nestedSnapshots.length);

        @SuppressWarnings("unchecked")
        TypeSerializerSnapshot<T> snapshot = (TypeSerializerSnapshot<T>) nestedSnapshots[pos];

        return snapshot.restoreSerializer();
    }

    public <T> TypeSerializerSchemaCompatibility<T> resolveCompatibilityWithNested(
            TypeSerializerSchemaCompatibility<?> outerCompatibility,
            TypeSerializer<?>... newNestedSerializers) {

        checkArgument(newNestedSerializers.length == nestedSnapshots.length,
                "Different number of new serializers and existing serializer configuration snapshots");

        // compatibility of the outer serializer's format
        if (outerCompatibility.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // check nested serializers for compatibility
        boolean nestedSerializerRequiresMigration = false;
        for (int i = 0; i < nestedSnapshots.length; i++) {
            TypeSerializerSchemaCompatibility<?> compatibility =
                    resolveCompatibility(newNestedSerializers[i], nestedSnapshots[i]);

            if (compatibility.isIncompatible()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            if (compatibility.isCompatibleAfterMigration()) {
                nestedSerializerRequiresMigration = true;
            }
        }

        return (nestedSerializerRequiresMigration || !outerCompatibility.isCompatibleAsIs()) ?
                TypeSerializerSchemaCompatibility.compatibleAfterMigration() :
                TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    public final void writeCompositeSnapshot(DataOutputView out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(VERSION);

        out.writeInt(nestedSnapshots.length);
        for (TypeSerializerSnapshot<?> snap : nestedSnapshots) {
            TypeSerializerSnapshot.writeVersionedSnapshot(out, snap);
        }
    }

    /**
     * Reads the composite snapshot of all the contained serializers.
     */
    public static CompositeSerializerSnapshot readCompositeSnapshot(DataInputView in, ClassLoader cl) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(String.format("Corrupt data, magic number mismatch. Expected %8x, found %8x",
                    MAGIC_NUMBER, magicNumber));
        }

        final int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }

        final int numSnapshots = in.readInt();
        final TypeSerializerSnapshot<?>[] nestedSnapshots = new TypeSerializerSnapshot<?>[numSnapshots];

        for (int i = 0; i < numSnapshots; i++) {
            nestedSnapshots[i] = TypeSerializerSnapshot.readVersionedSnapshot(in, cl);
        }

        return new CompositeSerializerSnapshot(nestedSnapshots);
    }

    /**
     * Reads the composite snapshot of all the contained serializers in a way that is compatible
     * with Version 1 of the deprecated {@link }.
     */
    public static CompositeSerializerSnapshot legacyReadProductSnapshots(DataInputView in, ClassLoader cl) throws IOException {
        @SuppressWarnings("deprecation")
        final List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializersAndSnapshots =
                TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(in, cl);

        final TypeSerializerSnapshot<?>[] nestedSnapshots = serializersAndSnapshots.stream()
                .map(t -> t.f1)
                .toArray(TypeSerializerSnapshot<?>[]::new);

        return new CompositeSerializerSnapshot(nestedSnapshots);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Utility method to conjure up a new scope for the generic parameters.
     */
    @SuppressWarnings("unchecked")
    private static <E> TypeSerializerSchemaCompatibility<E> resolveCompatibility(
            TypeSerializer<?> serializer,
            TypeSerializerSnapshot<?> snapshot) {

        TypeSerializer<E> typedSerializer = (TypeSerializer<E>) serializer;
        TypeSerializerSnapshot<E> typedSnapshot = (TypeSerializerSnapshot<E>) snapshot;

        return typedSnapshot.resolveSchemaCompatibility(typedSerializer);
    }

    private static TypeSerializer<?>[] snapshotsToRestoreSerializers(TypeSerializerSnapshot<?>... snapshots) {
        return Arrays.stream(snapshots)
                .map(TypeSerializerSnapshot::restoreSerializer)
                .toArray(TypeSerializer[]::new);
    }

}
