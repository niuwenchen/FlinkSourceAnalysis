package com.jackniu.flink.runtime.state;

import com.jackniu.flink.util.FutureUtil;
import com.jackniu.flink.util.LambdaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class StateUtil {
    private static final Logger LOG = LoggerFactory.getLogger(StateUtil.class);

    private StateUtil() {
        throw new AssertionError();
    }

    /**
     * Returns the size of a state object
     *
     * @param handle The handle to the retrieved state
     */
    public static long getStateSize(StateObject handle) {
        return handle == null ? 0 : handle.getStateSize();
    }

    /**
     * Iterates through the passed state handles and calls discardState() on each handle that is not null. All
     * occurring exceptions are suppressed and collected until the iteration is over and emitted as a single exception.
     *
     * @param handlesToDiscard State handles to discard. Passed iterable is allowed to deliver null values.
     * @throws Exception exception that is a collection of all suppressed exceptions that were caught during iteration
     */
    public static void bestEffortDiscardAllStateObjects(
            Iterable<? extends StateObject> handlesToDiscard) throws Exception {
        LambdaUtil.applyToAllWhileSuppressingExceptions(handlesToDiscard, StateObject::discardState);
    }

    /**
     * Discards the given state future by first trying to cancel it. If this is not possible, then
     * the state object contained in the future is calculated and afterwards discarded.
     *
     * @param stateFuture to be discarded
     * @throws Exception if the discard operation failed
     */
    public static void discardStateFuture(RunnableFuture<? extends StateObject> stateFuture) throws Exception {
        if (null != stateFuture) {
            if (!stateFuture.cancel(true)) {

                try {
                    // We attempt to get a result, in case the future completed before cancellation.
                    StateObject stateObject = FutureUtil.runIfNotDoneAndGet(stateFuture);

                    if (null != stateObject) {
                        stateObject.discardState();
                    }
                } catch (CancellationException | ExecutionException ex) {
                    LOG.debug("Cancelled execution of snapshot future runnable. Cancellation produced the following " +
                            "exception, which is expected an can be ignored.", ex);
                }
            }
        }
    }
}
