package com.jackniu.flink.api.common.functions;

import com.jackniu.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/8.
 */
public abstract class AbstractRichFunction implements RichFunction, Serializable {

    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------
    //  Runtime context access
    // --------------------------------------------------------------------------------------------

    private transient RuntimeContext runtimeContext;

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        this.runtimeContext = t;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        } else {
            throw new IllegalStateException("The runtime context has not been initialized.");
        }
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        if (this.runtimeContext == null) {
            throw new IllegalStateException("The runtime context has not been initialized.");
        } else if (this.runtimeContext instanceof IterationRuntimeContext) {
            return (IterationRuntimeContext) this.runtimeContext;
        } else {
            throw new IllegalStateException("This stub is not part of an iteration step function.");
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Default life cycle methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void open(Configuration parameters) throws Exception {}

    @Override
    public void close() throws Exception {}
}
