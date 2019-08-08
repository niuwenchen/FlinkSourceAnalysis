package com.jackniu.flink.util;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class AtomicDisposableReferenceCounter {
    private final Object lock = new Object();

    private int referenceCount;

    private boolean isDisposed;

    /** Enter the disposed state when the reference count reaches this number. */
    private final int disposeOnReferenceCount;

    public AtomicDisposableReferenceCounter() {
        this.disposeOnReferenceCount = 0;
    }

    public AtomicDisposableReferenceCounter(int disposeOnReferenceCount) {
        this.disposeOnReferenceCount = disposeOnReferenceCount;
    }

    /**
     * Increments the reference count and returns whether it was successful.
     * <p>
     * If the method returns <code>false</code>, the counter has already been disposed. Otherwise it
     * returns <code>true</code>.
     */
    public boolean increment() {
        synchronized (lock) {
            if (isDisposed) {
                return false;
            }

            referenceCount++;
            return true;
        }
    }

    /**
     * Decrements the reference count and returns whether the reference counter entered the disposed
     * state.
     * <p>
     * If the method returns <code>true</code>, the decrement operation disposed the counter.
     * Otherwise it returns <code>false</code>.
     */
    public boolean decrement() {
        synchronized (lock) {
            if (isDisposed) {
                return false;
            }

            referenceCount--;

            if (referenceCount <= disposeOnReferenceCount) {
                isDisposed = true;
            }

            return isDisposed;
        }
    }

    public int get() {
        synchronized (lock) {
            return referenceCount;
        }
    }

    /**
     * Returns whether the reference count has reached the disposed state.
     */
    public boolean isDisposed() {
        synchronized (lock) {
            return isDisposed;
        }
    }

    public boolean disposeIfNotUsed() {
        synchronized (lock) {
            if (referenceCount <= disposeOnReferenceCount) {
                isDisposed = true;
            }

            return isDisposed;
        }
    }
}
