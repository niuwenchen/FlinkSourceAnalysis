package com.jackniu.flink.runtime.checkpoint;

import com.jackniu.flink.runtime.jobgraph.OperatorID;
import com.jackniu.flink.runtime.state.CompositeStateHandle;
import com.jackniu.flink.runtime.state.SharedStateRegistry;
import com.jackniu.flink.util.Preconditions;

import java.util.*;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class OperatorState implements CompositeStateHandle {
    private static final long serialVersionUID = -4845578005863201810L;

    /** id of the operator */
    private final OperatorID operatorID;

    /** handles to non-partitioned states, subtaskindex -> subtaskstate */
    private final Map<Integer, OperatorSubtaskState> operatorSubtaskStates;

    /** parallelism of the operator when it was checkpointed */
    private final int parallelism;

    /** maximum parallelism of the operator when the job was first created */
    private final int maxParallelism;

    public OperatorState(OperatorID operatorID, int parallelism, int maxParallelism) {
        Preconditions.checkArgument(
                parallelism <= maxParallelism,
                "Parallelism " + parallelism + " is not smaller or equal to max parallelism " + maxParallelism + ".");

        this.operatorID = operatorID;

        this.operatorSubtaskStates = new HashMap<>(parallelism);

        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
    }

    public OperatorID getOperatorID() {
        return operatorID;
    }

    public void putState(int subtaskIndex, OperatorSubtaskState subtaskState) {
        Preconditions.checkNotNull(subtaskState);

        if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
            throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
                    " exceeds the maximum number of sub tasks " + operatorSubtaskStates.size());
        } else {
            operatorSubtaskStates.put(subtaskIndex, subtaskState);
        }
    }

    public OperatorSubtaskState getState(int subtaskIndex) {
        if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
            throw new IndexOutOfBoundsException("The given sub task index " + subtaskIndex +
                    " exceeds the maximum number of sub tasks " + operatorSubtaskStates.size());
        } else {
            return operatorSubtaskStates.get(subtaskIndex);
        }
    }

    public Collection<OperatorSubtaskState> getStates() {
        return operatorSubtaskStates.values();
    }

    public int getNumberCollectedStates() {
        return operatorSubtaskStates.size();
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    @Override
    public void discardState() throws Exception {
        for (OperatorSubtaskState operatorSubtaskState : operatorSubtaskStates.values()) {
            operatorSubtaskState.discardState();
        }
    }

    @Override
    public void registerSharedStates(SharedStateRegistry sharedStateRegistry) {
        for (OperatorSubtaskState operatorSubtaskState : operatorSubtaskStates.values()) {
            operatorSubtaskState.registerSharedStates(sharedStateRegistry);
        }
    }

    @Override
    public long getStateSize() {
        long result = 0L;

        for (int i = 0; i < parallelism; i++) {
            OperatorSubtaskState operatorSubtaskState = operatorSubtaskStates.get(i);
            if (operatorSubtaskState != null) {
                result += operatorSubtaskState.getStateSize();
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OperatorState) {
            OperatorState other = (OperatorState) obj;

            return operatorID.equals(other.operatorID)
                    && parallelism == other.parallelism
                    && operatorSubtaskStates.equals(other.operatorSubtaskStates);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return parallelism + 31 * Objects.hash(operatorID, operatorSubtaskStates);
    }

    public Map<Integer, OperatorSubtaskState> getSubtaskStates() {
        return Collections.unmodifiableMap(operatorSubtaskStates);
    }

    @Override
    public String toString() {
        // KvStates are always null in 1.1. Don't print this as it might
        // confuse users that don't care about how we store it internally.
        return "OperatorState(" +
                "operatorID: " + operatorID +
                ", parallelism: " + parallelism +
                ", maxParallelism: " + maxParallelism +
                ", sub task states: " + operatorSubtaskStates.size() +
                ", total size (bytes): " + getStateSize() +
                ')';
    }
}
