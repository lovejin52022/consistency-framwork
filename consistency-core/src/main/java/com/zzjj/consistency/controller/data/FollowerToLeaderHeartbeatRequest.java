package com.zzjj.consistency.controller.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * follower发送给leader的心跳请求
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FollowerToLeaderHeartbeatRequest {

    /**
     * 节点id
     */
    private String peerId;

}
