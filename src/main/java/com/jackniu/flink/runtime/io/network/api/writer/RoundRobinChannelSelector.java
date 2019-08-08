package com.jackniu.flink.runtime.io.network.api.writer;

import com.jackniu.flink.core.io.IOReadableWritable;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class RoundRobinChannelSelector<T extends IOReadableWritable> implements ChannelSelector<T> {

    /**
     * Stores the index of the channel to send the next record to.
     */
    private final int[] nextChannelToSendTo = new int[1];

    /**
     * Constructs a new default channel selector.
     */
    public RoundRobinChannelSelector() {
        this.nextChannelToSendTo[0] = 0;
    }

    @Override
    public int[] selectChannels(final T record, final int numberOfOutputChannels) {

        int newChannel = ++this.nextChannelToSendTo[0];
        if (newChannel >= numberOfOutputChannels) {
            this.nextChannelToSendTo[0] = 0;
        }

        return this.nextChannelToSendTo;
    }
}
