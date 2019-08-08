package com.jackniu.flink.util;

import com.jackniu.flink.util.function.CheckedSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class OptionalFailure<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nullable
    private transient T value;

    @Nullable
    private Throwable failureCause;

    private OptionalFailure(@Nullable T value, @Nullable Throwable failureCause) {
        this.value = value;
        this.failureCause = failureCause;
    }

    public static <T> OptionalFailure<T> of(T value) {
        return new OptionalFailure<>(value, null);
    }

    public static <T> OptionalFailure<T> ofFailure(Throwable failureCause) {
        return new OptionalFailure<>(null, failureCause);
    }

    /**
     * @return wrapped {@link OptionalFailure} returned by {@code valueSupplier} or wrapped failure if
     * {@code valueSupplier} has thrown an {@link Exception}.
     */
    public static <T> OptionalFailure<T> createFrom(CheckedSupplier<T> valueSupplier) {
        try {
            return of(valueSupplier.get());
        } catch (Exception ex) {
            return ofFailure(ex);
        }
    }

    /**
     * @return stored value or throw a {@link FlinkException} with {@code failureCause}.
     */
    public T get() throws FlinkException {
        if (value != null) {
            return value;
        }
        checkNotNull(failureCause);
        throw new FlinkException(failureCause);
    }

    /**
     * @return same as {@link #get()} but throws a {@link FlinkRuntimeException}.
     */
    public T getUnchecked() throws FlinkRuntimeException {
        if (value != null) {
            return value;
        }
        checkNotNull(failureCause);
        throw new FlinkRuntimeException(failureCause);
    }

    public Throwable getFailureCause() {
        return checkNotNull(failureCause);
    }

    public boolean isFailure() {
        return failureCause != null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, failureCause);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof OptionalFailure<?>)) {
            return false;
        }
        OptionalFailure<?> other = (OptionalFailure<?>) obj;
        return Objects.equals(value, other.value) &&
                Objects.equals(failureCause, other.failureCause);
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        stream.defaultWriteObject();
        stream.writeObject(value);
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        value = (T) stream.readObject();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{value=" + value + ", failureCause=" + failureCause + "}";
    }
}

