package com.zzjj.consistency.custom.shard;

/**
 * 分片建生成器
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public interface ShardingKeyGenerator {

    /**
     * 生产一致性任务分片键
     *
     * @return 一致性任务分片键
     */
    long generateShardKey();

}
