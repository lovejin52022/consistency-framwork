package com.zzjj.consistency.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * 调度器相关配置
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@ConfigurationProperties(prefix = "consistency.scheduler")
public class SchedulerConfigProperties {

    /**
     * [单位秒] leader检测follower是否存活的调度器每隔多长时间执行一次检查
     */
    public Integer followerAliveCheckIntervalSeconds = 10;
    /**
     * [单位秒] leader判定follower宕机的阈值
     */
    private Integer judgeFollowerDownSecondsThreshold = 120;
    /**
     * [单位秒] follower用于检测leader是否存活的调度器每隔多长时间执行一次检查
     */
    public Integer leaderAliveCheckIntervalSeconds = 10;
    /**
     * [单位秒] follower判定leader宕机的阈值
     */
    private Integer judgeLeaderDownSecondsThreshold = 120;
    /**
     * [单位秒] leader定时发给follower的心跳的调度器，同时也会将leader对任务的分片信息发送给各个follower节点每隔多长时间执行一次
     */
    public Integer leaderToFollowerHeartbeatIntervalSeconds = 10;
    /**
     * [单位秒] follower对leader发送心跳的调度器
     */
    public Integer followerHeartbeatIntervalSeconds = 10;
    /**
     * [单位秒] 一致性框架自身的执行任务时的调度器执行任务的频率，每隔多长时间调度一次
     */
    public Integer consistencyTaskExecuteIntervalSeconds = 10;

}
