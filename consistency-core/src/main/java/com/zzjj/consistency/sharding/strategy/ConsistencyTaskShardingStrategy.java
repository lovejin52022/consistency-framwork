package com.zzjj.consistency.sharding.strategy;

import java.util.List;
import java.util.Map;

/**
 * 分片策略接口
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public interface ConsistencyTaskShardingStrategy {

    /**
     * 任务分片.
     *
     * @param taskInstances 所有参与分片的机器（实例列表）
     * @param shardingTotalCount 分片总数
     * @return 分片结果 格式如下："192.168.0.222:8080:1":[0],"192.168.0.222:8081:2":[1],"192.168.0.222:8082:3":[2]
     */
    Map<String, List<Long>> sharding(List<String> taskInstances, long shardingTotalCount);

}
