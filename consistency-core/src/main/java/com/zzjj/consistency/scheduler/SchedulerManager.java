package com.zzjj.consistency.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import com.zzjj.consistency.config.ConsistencyConfiguration;
import com.zzjj.consistency.scheduler.follower.FollowerHeartbeatScheduler;
import com.zzjj.consistency.scheduler.follower.LeaderAliveCheckScheduler;
import com.zzjj.consistency.scheduler.leader.FollowerAliveCheckScheduler;
import com.zzjj.consistency.scheduler.leader.LeaderToFollowerHeartbeatScheduler;

import lombok.extern.slf4j.Slf4j;

/**
 * 调度管理
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@Component
public class SchedulerManager implements Scheduler {
    /**
     * leader检测follower是否存活的调度器所使用的线程
     */
    private final ScheduledExecutorService followerAliveCheckScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "followerActiveScheduler"));
    /**
     * leader发送给follower心跳调度所用的线程
     */
    private final ScheduledExecutorService heartbeatScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "leaderHeartScheduler"));
    /**
     * 一致性任务调度线程
     */
    private final ScheduledExecutorService taskScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "taskScheduler"));
    /**
     * leader是否存活的线程
     */
    private final ScheduledExecutorService leaderAliveScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "leaderAliveScheduler"));
    /**
     * follower向leader发送的心跳线程
     */
    private final ScheduledExecutorService followerHeartbeatScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "followerHeartbeat"));

    /**
     * leader节点专用 leader定时发给follower的心跳的调度器，同时也会将leader对任务的分片信息发送给各个follower节点
     */
    private LeaderToFollowerHeartbeatScheduler leaderToFollowerHeartbeatScheduler;
    /**
     * leader节点专用 leader检测follower是否存活的调度器
     */
    private FollowerAliveCheckScheduler followerAliveCheckScheduler;
    /**
     * follower专用 follower对leader心跳响应超时检测使用的调度器
     */
    private FollowerHeartbeatScheduler followerHeartbeatTaskScheduler;
    /**
     * follower专用 follower用于检测leader是否存活的调度器
     */
    private LeaderAliveCheckScheduler leaderAliveCheckScheduler;
    /**
     * 一致性任务执行 所使用的调度器，leader和follower都会使用
     */
    private ConsistencyTaskScheduler consistencyTaskScheduler;
    /**
     * 一致性任务框架配置
     */
    @Autowired
    private ConsistencyConfiguration consistencyConfiguration;

    /**
     * 创建leader检测follower是否存活的调度器
     *
     * @param task 任务task
     * @return 选举超时调度器
     */
    @Override
    public FollowerAliveCheckScheduler createFollowerAliveCheckScheduler(final Runnable task) {
        if (!ObjectUtils.isEmpty(this.followerAliveCheckScheduler)) {
            this.followerAliveCheckScheduler.cancel();
        }
        final ScheduledFuture<?> scheduledFuture =
            this.followerAliveCheckScheduledExecutorService.scheduleWithFixedDelay(task, 10,
                this.consistencyConfiguration.getFollowerAliveCheckIntervalSeconds(), TimeUnit.SECONDS);
        this.followerAliveCheckScheduler = new FollowerAliveCheckScheduler(scheduledFuture);
        return this.followerAliveCheckScheduler;
    }

    /**
     * 创建leader定时发给follower的心跳的调度器，同时也会将leader对任务的分片信息发送给各个follower节点
     *
     * @param task 心跳任务
     * @return 日志复制任务调度器
     */
    @Override
    public LeaderToFollowerHeartbeatScheduler createLeaderToFollowerHeartbeatScheduler(final Runnable task) {
        if (!ObjectUtils.isEmpty(this.leaderToFollowerHeartbeatScheduler)) {
            this.leaderToFollowerHeartbeatScheduler.cancel();
        }
        // 你作为leader来说，你只要有一个线程不停的调度，不停的发送心跳给所有的follower就可以了
        // leader每隔10s，会去给follower发送一次心跳
        final ScheduledFuture<?> scheduledFuture = this.heartbeatScheduledExecutorService.scheduleWithFixedDelay(task,
            0, this.consistencyConfiguration.getLeaderToFollowerHeartbeatIntervalSeconds(), TimeUnit.SECONDS);
        this.leaderToFollowerHeartbeatScheduler = new LeaderToFollowerHeartbeatScheduler(scheduledFuture);
        return this.leaderToFollowerHeartbeatScheduler;
    }

    /**
     * 任务执行调度器
     *
     * @param task 一致性任务调度器
     * @return 一致性任务
     */
    @Override
    public ConsistencyTaskScheduler createConsistencyTaskScheduler(final Runnable task) {
        if (!ObjectUtils.isEmpty(this.consistencyTaskScheduler)) {
            this.consistencyTaskScheduler.cancel();
        }
        final ScheduledFuture<?> scheduledFuture = this.taskScheduledExecutorService.scheduleWithFixedDelay(task, 0,
            this.consistencyConfiguration.getConsistencyTaskExecuteIntervalSeconds(), TimeUnit.SECONDS);
        this.consistencyTaskScheduler = new ConsistencyTaskScheduler(scheduledFuture);
        return this.consistencyTaskScheduler;
    }

    /**
     * 创建follower对leader发送心跳的调度器
     *
     * @param task 心跳任务
     * @return follower心跳任务调度器
     */
    @Override
    public FollowerHeartbeatScheduler createFollowerHeartbeatScheduler(final Runnable task) {
        if (!ObjectUtils.isEmpty(this.followerHeartbeatTaskScheduler)) {
            this.followerHeartbeatTaskScheduler.cancel();
        }
        final ScheduledFuture<?> scheduledFuture =
            this.followerHeartbeatScheduledExecutorService.scheduleWithFixedDelay(task, 0,
                this.consistencyConfiguration.getFollowerHeartbeatIntervalSeconds(), TimeUnit.SECONDS);
        this.followerHeartbeatTaskScheduler = new FollowerHeartbeatScheduler(scheduledFuture);
        return this.followerHeartbeatTaskScheduler;
    }

    /**
     * 创建follower用于检测leader是否存活的调度器
     *
     * @param task 任务
     * @return follower用于检测leader是否存活的调度器
     */
    @Override
    public LeaderAliveCheckScheduler createLeaderAliveScheduler(final Runnable task) {
        if (!ObjectUtils.isEmpty(this.leaderAliveCheckScheduler)) {
            this.leaderAliveCheckScheduler.cancel();
        }
        final ScheduledFuture<?> scheduledFuture = this.leaderAliveScheduledExecutorService.scheduleWithFixedDelay(task,
            0, this.consistencyConfiguration.getLeaderAliveCheckIntervalSeconds(), TimeUnit.SECONDS);
        this.leaderAliveCheckScheduler = new LeaderAliveCheckScheduler(scheduledFuture);
        return this.leaderAliveCheckScheduler;
    }

    /**
     * 取消所有调度任务
     */
    @Override
    public void cancelAllScheduler() {
        // leader他会不断的给follower发送心跳，follower也会不断地给leader发送心跳
        // leader来说，他需要定时检查follower发送过来的心跳，follower是否宕机
        // follower来说，他需要定时检查leader发送过来的心跳，leader是否宕机
        if (!ObjectUtils.isEmpty(this.followerAliveCheckScheduler)) {
            this.followerAliveCheckScheduler.cancel();
        }
        if (!ObjectUtils.isEmpty(this.leaderToFollowerHeartbeatScheduler)) {
            this.leaderToFollowerHeartbeatScheduler.cancel();
        }
        if (!ObjectUtils.isEmpty(this.followerHeartbeatTaskScheduler)) {
            this.followerHeartbeatTaskScheduler.cancel();
        }
        if (!ObjectUtils.isEmpty(this.leaderAliveCheckScheduler)) {
            this.leaderAliveCheckScheduler.cancel();
        }
    }

}
