package com.jackniu.flink.core.io;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface InputSplitSource<T extends InputSplit> extends Serializable {
    T[] createInputSplits(int minNumSplits) throws Exception;

    /**
     * Returns the assigner for the input splits. Assigner determines which parallel instance of the
     * input format gets which input split.
     *
     * @return The input split assigner.
     */
    InputSplitAssigner getInputSplitAssigner(T[] inputSplits);
}
