package com.jackniu.flink.runtime.operators.util.metrics;

import com.jackniu.flink.metrics.Counter;
import com.jackniu.flink.util.Collector;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class CountingCollector<OUT> implements Collector<OUT> {
    private final Collector<OUT> collector;
    private final Counter numRecordsOut;

    public CountingCollector(Collector<OUT> collector, Counter numRecordsOut) {
        this.collector = collector;
        this.numRecordsOut = numRecordsOut;
    }

    @Override
    public void collect(OUT record) {
        this.numRecordsOut.inc();
        this.collector.collect(record);
    }

    @Override
    public void close() {
        this.collector.close();
    }
}
