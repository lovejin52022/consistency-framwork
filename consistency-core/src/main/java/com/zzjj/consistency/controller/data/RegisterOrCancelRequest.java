package com.zzjj.consistency.controller.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 上线或下线请求
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RegisterOrCancelRequest {

    /**
     * follower的ip地址
     */
    private String ip;
    /**
     * follower的端口号
     */
    private String port;
    /**
     * follower的peerId
     */
    private String peerId;
    /**
     * 操作类型 1:上线 2:下线
     */
    private Integer opType;
    /**
     * 是否是leader节点下线
     */
    private boolean leaderOffline;

}
