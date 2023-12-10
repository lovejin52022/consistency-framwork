package com.zzjj.consistency.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * 一致性并行处理配置
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@ConfigurationProperties(prefix = "consistency.parallel.pool")
public class ConsistencyParallelTaskConfigProperties {

    /**
     * 调度型任务线程池的核心线程数
     */
    public Integer threadCorePoolSize = 5;
    /**
     * 调度型任务线程池的最大线程数
     */
    public Integer threadMaxPoolSize = 5;
    /**
     * 调度型任务线程池的队列大小
     */
    public Integer threadPoolQueueSize = 100;
    /**
     * 线程池中无任务时线程存活时间
     */
    public Long threadPoolKeepAliveTime = 60L;
    /**
     * 可选值:[SECONDS,MINUTES,HOURS,DAYS,NANOSECONDS,MICROSECONDS,MILLISECONDS] 线程池中无任务时线程存活时间单位
     */
    public String threadPoolKeepAliveTimeUnit = "SECONDS";
    /**
     * 这里要配置类型全路径且类要实现com.zzjj.consistency.custom.query.TaskTimeRangeQuery接口 如：com.xxx.TaskTimeLineQuery
     */
    private String taskScheduleTimeRangeClassName = "";

}
