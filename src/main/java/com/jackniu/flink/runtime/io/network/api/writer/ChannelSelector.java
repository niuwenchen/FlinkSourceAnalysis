package com.jackniu.flink.runtime.io.network.api.writer;

/**
 * Created by JackNiu on 2019/7/7.
 */

import com.jackniu.flink.core.io.IOReadableWritable;

public interface ChannelSelector<T extends IOReadableWritable> {

        /**
         * Returns the logical channel indexes, to which the given record should be
         * written.
         *
         * @param record      the record to the determine the output channels for
         * @param numChannels the total number of output channels which are attached to respective output gate
         * @return a (possibly empty) array of integer numbers which indicate the indices of the output channels through
         * which the record shall be forwarded
         */
        int[] selectChannels(T record, int numChannels);
        }
