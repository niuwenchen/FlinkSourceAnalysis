package com.jackniu.flink.api.common.state;

import com.jackniu.flink.api.common.time.Time;
import com.jackniu.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.EnumMap;

import static com.jackniu.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;
import static com.jackniu.flink.api.common.state.StateTtlConfig.TimeCharacteristic.ProcessingTime;
import static com.jackniu.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite;

/**
 * Created by JackNiu on 2019/7/5.
 */
public class StateTtlConfig  implements Serializable {
    private static final long serialVersionUID = -7592693245044289793L;

    public static final StateTtlConfig DISABLED =
            newBuilder(Time.milliseconds(Long.MAX_VALUE)).setUpdateType(UpdateType.Disabled).build();

    /**
     * This option value configures when to update last access timestamp which prolongs state TTL.
     */
    public enum UpdateType {
        /** TTL is disabled. State does not expire. */
        Disabled,
        /** Last access timestamp is initialised when state is created and updated on every write operation. */
        OnCreateAndWrite,
        /** The same as <code>OnCreateAndWrite</code> but also updated on read. */
        OnReadAndWrite
    }

    /**
     * This option configures whether expired user value can be returned or not.
     */
    public enum StateVisibility {
        /** Return expired user value if it is not cleaned up yet. */
        ReturnExpiredIfNotCleanedUp,
        /** Never return expired user value. */
        NeverReturnExpired
    }

    /**
     * This option configures time scale to use for ttl.
     */
    public enum TimeCharacteristic {
        /** Processing time, see also <code>TimeCharacteristic.ProcessingTime</code>. */
        ProcessingTime
    }
    private final UpdateType updateType;
    private final StateVisibility stateVisibility;
    private final TimeCharacteristic timeCharacteristic;
    private final Time ttl;
    private final CleanupStrategies cleanupStrategies;

    private StateTtlConfig(
            UpdateType updateType,
            StateVisibility stateVisibility,
            TimeCharacteristic timeCharacteristic,
            Time ttl,
            CleanupStrategies cleanupStrategies) {
        this.updateType = Preconditions.checkNotNull(updateType);
        this.stateVisibility = Preconditions.checkNotNull(stateVisibility);
        this.timeCharacteristic = Preconditions.checkNotNull(timeCharacteristic);
        this.ttl = Preconditions.checkNotNull(ttl);
        this.cleanupStrategies = cleanupStrategies;
        Preconditions.checkArgument(ttl.toMilliseconds() > 0,
                "TTL is expected to be positive");
    }

    @Nonnull
    public UpdateType getUpdateType() {
        return updateType;
    }

    @Nonnull
    public StateVisibility getStateVisibility() {
        return stateVisibility;
    }

    @Nonnull
    public Time getTtl() {
        return ttl;
    }

    @Nonnull
    public TimeCharacteristic getTimeCharacteristic() {
        return timeCharacteristic;
    }

    public boolean isEnabled() {
        return updateType != UpdateType.Disabled;
    }

    @Nonnull
    public CleanupStrategies getCleanupStrategies() {
        return cleanupStrategies;
    }

    @Override
    public String toString() {
        return "StateTtlConfig{" +
                "updateType=" + updateType +
                ", stateVisibility=" + stateVisibility +
                ", timeCharacteristic=" + timeCharacteristic +
                ", ttl=" + ttl +
                '}';
    }

    @Nonnull
    public static Builder newBuilder(@Nonnull Time ttl) {
        return new Builder(ttl);
    }

    /**
     * Builder for the {@link StateTtlConfig}.
     */
    public static class Builder {

        private UpdateType updateType = OnCreateAndWrite;
        private StateVisibility stateVisibility = NeverReturnExpired;
        private TimeCharacteristic timeCharacteristic = ProcessingTime;
        private Time ttl;
        private CleanupStrategies cleanupStrategies = new CleanupStrategies();

        public Builder(@Nonnull Time ttl) {
            this.ttl = ttl;
        }

        /**
         * Sets the ttl update type.
         *
         * @param updateType The ttl update type configures when to update last access timestamp which prolongs state TTL.
         */
        @Nonnull
        public Builder setUpdateType(UpdateType updateType) {
            this.updateType = updateType;
            return this;
        }

        @Nonnull
        public Builder updateTtlOnCreateAndWrite() {
            return setUpdateType(OnCreateAndWrite);
        }

        @Nonnull
        public Builder updateTtlOnReadAndWrite() {
            return setUpdateType(UpdateType.OnReadAndWrite);
        }

        /**
         * Sets the state visibility.
         *
         * @param stateVisibility The state visibility configures whether expired user value can be returned or not.
         */
        @Nonnull
        public Builder setStateVisibility(@Nonnull StateVisibility stateVisibility) {
            this.stateVisibility = stateVisibility;
            return this;
        }

        @Nonnull
        public Builder returnExpiredIfNotCleanedUp() {
            return setStateVisibility(StateVisibility.ReturnExpiredIfNotCleanedUp);
        }

        @Nonnull
        public Builder neverReturnExpired() {
            return setStateVisibility(NeverReturnExpired);
        }

        /**
         * Sets the time characteristic.
         *
         * @param timeCharacteristic The time characteristic configures time scale to use for ttl.
         */
        @Nonnull
        public Builder setTimeCharacteristic(@Nonnull TimeCharacteristic timeCharacteristic) {
            this.timeCharacteristic = timeCharacteristic;
            return this;
        }

        @Nonnull
        public Builder useProcessingTime() {
            return setTimeCharacteristic(ProcessingTime);
        }

        /** Cleanup expired state in full snapshot on checkpoint. */
        @Nonnull
        public Builder cleanupFullSnapshot() {
            cleanupStrategies.strategies.put(
                    CleanupStrategies.Strategies.FULL_STATE_SCAN_SNAPSHOT,
                    new CleanupStrategies.CleanupStrategy() {  });
            return this;
        }

        /**
         * Sets the ttl time.
         * @param ttl The ttl time.
         */
        @Nonnull
        public Builder setTtl(@Nonnull Time ttl) {
            this.ttl = ttl;
            return this;
        }

        @Nonnull
        public StateTtlConfig build() {
            return new StateTtlConfig(
                    updateType,
                    stateVisibility,
                    timeCharacteristic,
                    ttl,
                    cleanupStrategies);
        }
    }

    /**
     * TTL cleanup strategies.
     *
     * <p>This class configures when to cleanup expired state with TTL.
     * By default, state is always cleaned up on explicit read access if found expired.
     * Currently cleanup of state full snapshot can be additionally activated.
     */
    public static class CleanupStrategies implements Serializable {
        private static final long serialVersionUID = -1617740467277313524L;

        /** Fixed strategies ordinals in {@code strategies} config field. */
        enum Strategies {
            FULL_STATE_SCAN_SNAPSHOT
        }

        /** Base interface for cleanup strategies configurations. */
        interface CleanupStrategy extends Serializable {

        }

        final EnumMap<Strategies, CleanupStrategy> strategies = new EnumMap<>(Strategies.class);

        public boolean inFullSnapshot() {
            return strategies.containsKey(Strategies.FULL_STATE_SCAN_SNAPSHOT);
        }
    }

}
