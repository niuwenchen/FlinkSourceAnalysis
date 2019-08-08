package com.jackniu.flink.runtime.state;

import com.jackniu.flink.core.fs.FSDataInputStream;

/**
 * Created by JackNiu on 2019/7/6.
 */
public class PlaceholderStreamStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;

    public PlaceholderStreamStateHandle() {
    }

    @Override
    public FSDataInputStream openInputStream() {
        throw new UnsupportedOperationException(
                "This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
    }

    @Override
    public void discardState() throws Exception {
        // nothing to do.
    }

    @Override
    public long getStateSize() {
        return 0L;
    }
}

