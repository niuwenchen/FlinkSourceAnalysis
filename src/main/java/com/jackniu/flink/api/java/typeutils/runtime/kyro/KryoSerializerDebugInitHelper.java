package com.jackniu.flink.api.java.typeutils.runtime.kyro;

/**
 * Created by JackNiu on 2019/6/26.
 */
public class KryoSerializerDebugInitHelper {
    /** This captures the initial setting after initialization. It is used to
     * validate in tests that we never change the default to true. */
    static final boolean INITIAL_SETTING;

    /** The flag that is used to initialize the KryoSerializer's concurrency check flag. */
    static boolean setToDebug = false;

    static {
        // capture the default setting, for tests
        INITIAL_SETTING = setToDebug;

        // if assertions are active, the check should be activated
        //noinspection AssertWithSideEffects,ConstantConditions
        assert setToDebug = true;
    }
}
