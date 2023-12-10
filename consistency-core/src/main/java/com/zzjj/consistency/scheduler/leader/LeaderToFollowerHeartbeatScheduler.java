package com.zzjj.consistency.scheduler.leader;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * leader定时发给follower的心跳的调度器，同时也会将leader对任务的分片信息发送给各个follower节点
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public class LeaderToFollowerHeartbeatScheduler {

    /**
     * 调度的future
     */
    private final ScheduledFuture<?> scheduledFuture;

    public LeaderToFollowerHeartbeatScheduler(final ScheduledFuture<?> scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
    }

    /**
     * 取消心跳调度任务
     */
    public void cancel() {
        this.scheduledFuture.cancel(false);
    }

    @Override
    public String toString() {
        return "LeaderToFollowerHeartbeatScheduler{" + "scheduledFuture="
            + this.scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + " ms" + '}';
    }

    public ScheduledFuture<?> getScheduledFuture() {
        return this.scheduledFuture;
    }
}
