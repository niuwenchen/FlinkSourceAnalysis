package com.jackniu.flink.api.common.io;

import com.jackniu.flink.api.common.io.statistics.BaseStatistics;
import com.jackniu.flink.configuration.Configuration;
import com.jackniu.flink.core.io.InputSplit;
import com.jackniu.flink.core.io.InputSplitAssigner;
import com.jackniu.flink.core.io.InputSplitSource;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by JackNiu on 2019/6/24.
 */
public interface InputFormat<OT, T extends InputSplit> extends InputSplitSource<T>, Serializable {
    void configure(Configuration parameters);
    BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException;
    @Override
    T[] createInputSplits(int minNumSplits) throws IOException;
    @Override
    InputSplitAssigner getInputSplitAssigner(T[] inputSplits);
    void open(T split) throws IOException;
    boolean reachedEnd() throws IOException;
    OT nextRecord(OT reuse) throws IOException;
    void close() throws IOException;

}
