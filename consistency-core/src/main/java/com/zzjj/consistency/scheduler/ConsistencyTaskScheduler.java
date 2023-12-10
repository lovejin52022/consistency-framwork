package com.zzjj.consistency.scheduler;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 一致性框架任务调度器 leader follower都会使用
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public class ConsistencyTaskScheduler {

    /**
     * 调度的future
     */
    private final ScheduledFuture<?> scheduledFuture;

    public ConsistencyTaskScheduler(final ScheduledFuture<?> scheduledFuture) {
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
        return "ConsistencyTaskTimer{" + "scheduledFuture=" + this.scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + " ms"
            + '}';
    }

}
