package com.zzjj.consistency.controller.data;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * leader向follower发送的心跳请求, 携带分片结果
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LeaderToFollowerHeartbeatRequest {

    /**
     * leader节点的分片结果
     */
    private Map<String, List<Long>> cacheSharingResult;
    /**
     * 分片结果的md5值 用于follower节点进行校验 是否需要更新本地的分片结果
     */
    private String checksum;
    /**
     * 当前leader id
     */
    private String currentLeaderId;

}
