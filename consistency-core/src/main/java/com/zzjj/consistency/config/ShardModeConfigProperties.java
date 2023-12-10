package com.zzjj.consistency.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * 任务分片配置
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@ConfigurationProperties(prefix = "consistency.shard")
public class ShardModeConfigProperties {

    /**
     * 任务表是否进行分库
     */
    public Boolean taskSharded = false;
    /**
     * 生成任务表分片key的ClassName 这里要配置类型全路径且类要实现com.zzjj.consistency.custom.shard.ShardingKeyGenerator接口
     */
    private String shardingKeyGeneratorClassName = "";

    /**
     * 任务分片数
     */
    public Long taskShardingCount;

}
