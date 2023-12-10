package com.zzjj.consistency.scheduler.follower;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * follower用于检测leader是否存活的调度器
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public class LeaderAliveCheckScheduler {

    /**
     * 获取调度的future
     */
    private final ScheduledFuture<?> scheduledFuture;

    public LeaderAliveCheckScheduler(final ScheduledFuture<?> scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
    }

    /**
     * 取消
     */
    public void cancel() {
        this.scheduledFuture.cancel(false);
    }

    @Override
    public String toString() {
        if (this.scheduledFuture.isCancelled()) {
            return "LeaderAliveCheckScheduler已经被取消了";
        }

        if (this.scheduledFuture.isDone()) {
            return "LeaderAliveCheckScheduler已经调度完成了";
        }

        return "LeaderAliveCheckScheduler还没有执行, 将于" + this.scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + "ms 后执行";
    }
}
