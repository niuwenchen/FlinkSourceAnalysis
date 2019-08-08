package com.jackniu.flink.runtime.state;

import com.jackniu.flink.core.fs.FSDataInputStream;
import com.jackniu.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface OperatorStateHandle extends StreamStateHandle {
    /**
     * Returns a map of meta data for all contained states by their name.
     */
    Map<String, StateMetaInfo> getStateNameToPartitionOffsets();

    /**
     * Returns an input stream to read the operator state information.
     */
    @Override
    FSDataInputStream openInputStream() throws IOException;

    /**
     * Returns the underlying stream state handle that points to the state data.
     */
    StreamStateHandle getDelegateStateHandle();

    /**
     * The modes that determine how an {@link } is assigned to tasks during restore.
     */
    enum Mode {
        SPLIT_DISTRIBUTE,	// The operator state partitions in the state handle are split and distributed to one task each.
        UNION,				// The operator state partitions are UNION-ed upon restoring and sent to all tasks.
        BROADCAST			// The operator states are identical, as the state is produced from a broadcast stream.
    }

    /**
     * Meta information about the operator state handle.
     */
    class StateMetaInfo implements Serializable {

        private static final long serialVersionUID = 3593817615858941166L;

        private final long[] offsets;
        private final Mode distributionMode;

        public StateMetaInfo(long[] offsets, Mode distributionMode) {
            this.offsets = Preconditions.checkNotNull(offsets);
            this.distributionMode = Preconditions.checkNotNull(distributionMode);
        }

        public long[] getOffsets() {
            return offsets;
        }

        public Mode getDistributionMode() {
            return distributionMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StateMetaInfo that = (StateMetaInfo) o;

            return Arrays.equals(getOffsets(), that.getOffsets())
                    && getDistributionMode() == that.getDistributionMode();
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(getOffsets());
            result = 31 * result + getDistributionMode().hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "StateMetaInfo{" +
                    "offsets=" + Arrays.toString(offsets) +
                    ", distributionMode=" + distributionMode +
                    '}';
        }
    }
}
