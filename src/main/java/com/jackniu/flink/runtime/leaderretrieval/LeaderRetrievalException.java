package com.jackniu.flink.runtime.leaderretrieval;

import com.jackniu.flink.util.FlinkException;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class LeaderRetrievalException extends FlinkException {

    private static final long serialVersionUID = 42;

    public LeaderRetrievalException(String message) {
        super(message);
    }

    public LeaderRetrievalException(Throwable cause) {
        super(cause);
    }

    public LeaderRetrievalException(String message, Throwable cause) {
        super(message, cause);
    }
}
