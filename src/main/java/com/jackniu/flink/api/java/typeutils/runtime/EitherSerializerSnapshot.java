package com.jackniu.flink.api.java.typeutils.runtime;

import com.jackniu.flink.api.common.typeutils.CompositeSerializerSnapshot;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import com.jackniu.flink.api.common.typeutils.TypeSerializerSnapshot;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.types.Either;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.jackniu.flink.util.Preconditions.checkState;

/**
 * Created by JackNiu on 2019/7/1.
 */
public class EitherSerializerSnapshot<L, R> implements TypeSerializerSnapshot<Either<L, R>> {
    private static final int CURRENT_VERSION = 2;

    /** Snapshot handling for the component serializer snapshot. */
    @Nullable
    private CompositeSerializerSnapshot nestedSnapshot;

    /**
     * Constructor for read instantiation.
     */
    @SuppressWarnings("unused")
    public EitherSerializerSnapshot() {}

    /**
     * Constructor to create the snapshot for writing.
     */
    public EitherSerializerSnapshot(
            TypeSerializer<L> leftSerializer,
            TypeSerializer<R> rightSerializer) {

        this.nestedSnapshot = new CompositeSerializerSnapshot(leftSerializer, rightSerializer);
    }

    // ------------------------------------------------------------------------

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        checkState(nestedSnapshot != null);
        nestedSnapshot.writeCompositeSnapshot(out);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
        switch (readVersion) {
            case 1:
                readV1(in, classLoader);
                break;
            case 2:
                readV2(in, classLoader);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized version: " + readVersion);
        }
    }

    private void readV1(DataInputView in, ClassLoader classLoader) throws IOException {
        nestedSnapshot = CompositeSerializerSnapshot.legacyReadProductSnapshots(in, classLoader);
    }

    private void readV2(DataInputView in, ClassLoader classLoader) throws IOException {
        nestedSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, classLoader);
    }

    @Override
    public TypeSerializer<Either<L, R>> restoreSerializer() {
        checkState(nestedSnapshot != null);
        return new EitherSerializer<>(
                nestedSnapshot.getRestoreSerializer(0),
                nestedSnapshot.getRestoreSerializer(1));
    }

    @Override
    public TypeSerializerSchemaCompatibility<Either<L, R>> resolveSchemaCompatibility(
            TypeSerializer<Either<L, R>> newSerializer) {
        checkState(nestedSnapshot != null);

        if (newSerializer instanceof EitherSerializer) {
            EitherSerializer<L, R> serializer = (EitherSerializer<L, R>) newSerializer;

            return nestedSnapshot.resolveCompatibilityWithNested(
                    TypeSerializerSchemaCompatibility.compatibleAsIs(),
                    serializer.getLeftSerializer(),
                    serializer.getRightSerializer());
        }
        else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
