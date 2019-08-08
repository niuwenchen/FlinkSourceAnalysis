package com.jackniu.flink.runtime.leaderretrieval;

import javax.annotation.Nullable;
import java.util.UUID;

/**
 * Created by JackNiu on 2019/7/6.
 */
public interface LeaderRetrievalListener {
    /**
     * This method is called by the {@link LeaderRetrievalService} when a new leader is elected.
     *
     * @param leaderAddress The address of the new leader
     * @param leaderSessionID The new leader session ID
     */
    void notifyLeaderAddress(@Nullable String leaderAddress, @Nullable UUID leaderSessionID);

    /**
     * This method is called by the {@link LeaderRetrievalService} in case of an exception. This
     * assures that the {@link LeaderRetrievalListener} is aware of any problems occurring in the
     * {@link LeaderRetrievalService} thread.
     * @param exception
     */
    void handleError(Exception exception);
}
