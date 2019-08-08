package com.jackniu.flink.runtime.state;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface CompositeStateHandle extends StateObject{
    /**
     * Register both newly created and already referenced shared states in the given
     * {@link SharedStateRegistry}. This method is called when the checkpoint
     * successfully completes or is recovered from failures.
     * <p>
     * After this is completed, newly created shared state is considered as published is no longer
     * owned by this handle. This means that it should no longer be deleted as part of calls to
     * {@link #discardState()}. Instead, {@link #discardState()} will trigger an unregistration
     * from the registry.
     *
     * @param stateRegistry The registry where shared states are registered.
     */
    void registerSharedStates(SharedStateRegistry stateRegistry);
}
