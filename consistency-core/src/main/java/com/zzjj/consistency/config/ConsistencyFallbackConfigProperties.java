package com.zzjj.consistency.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * 一致性降级配置
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@ConfigurationProperties(prefix = "consistency.action")
public class ConsistencyFallbackConfigProperties {

    /**
     * 触发降级逻辑的阈值 任务执行次数 如果大于该值 就会进行降级
     */
    public Integer failCountThreshold = 0;

}
