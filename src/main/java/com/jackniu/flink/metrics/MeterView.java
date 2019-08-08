package com.jackniu.flink.metrics;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class MeterView implements Meter, View {
    /** The underlying counter maintaining the count. */
    private final Counter counter;
    /** The time-span over which the average is calculated. */
    private final int timeSpanInSeconds;
    /** Circular array containing the history of values. */
    private final long[] values;
    /** The index in the array for the current time. */
    private int time = 0;
    /** The last rate we computed. */
    private double currentRate = 0;

    public MeterView(int timeSpanInSeconds) {
        this(new SimpleCounter(), timeSpanInSeconds);
    }

    public MeterView(Counter counter, int timeSpanInSeconds) {
        this.counter = counter;
        // the time-span must be larger than the update-interval as otherwise the array has a size of 1,
        // for which no rate can be computed as no distinct before/after measurement exists.
        this.timeSpanInSeconds = Math.max(
                timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS),
                UPDATE_INTERVAL_SECONDS);
        this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
    }

    @Override
    public void markEvent() {
        this.counter.inc();
    }

    @Override
    public void markEvent(long n) {
        this.counter.inc(n);
    }

    @Override
    public long getCount() {
        return counter.getCount();
    }

    @Override
    public double getRate() {
        return currentRate;
    }

    @Override
    public void update() {
        time = (time + 1) % values.length;
        values[time] = counter.getCount();
        currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
    }
}
