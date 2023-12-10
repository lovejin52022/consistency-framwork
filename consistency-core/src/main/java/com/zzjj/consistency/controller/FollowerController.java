package com.zzjj.consistency.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.zzjj.consistency.common.CommonError;
import com.zzjj.consistency.common.CommonRes;
import com.zzjj.consistency.common.EmBusinessError;
import com.zzjj.consistency.controller.data.LeaderToFollowerHeartbeatRequest;
import com.zzjj.consistency.controller.data.LeaderToFollowerHeartbeatResponse;
import com.zzjj.consistency.election.PeerElectionHandler;
import com.zzjj.consistency.sharding.ConsistencyTaskShardingContext;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@RestController
@RequestMapping("/follower")
public class FollowerController {

    /**
     * 选举及任务分片处理器
     */
    @Autowired
    private PeerElectionHandler peerElectionHandler;

    @PostMapping("/heartbeat")
    public CommonRes<?>
        leaderHeartbeat(@RequestBody LeaderToFollowerHeartbeatRequest leaderToFollowerHeartbeatRequest) {
        log.info("follower收到来自leader的心跳包 {}", JSONUtil.toJsonStr(leaderToFollowerHeartbeatRequest));
        try {
            // 获取leader传递来的，分片上下文的校验和
            String checksum = leaderToFollowerHeartbeatRequest.getChecksum();
            // 获取分片上下文
            ConsistencyTaskShardingContext consistencyTaskShardingContext =
                    this.peerElectionHandler.getConsistencyTaskShardingContext();
            // 如果校验和为空或校验和与本地的不同，则进行变更
            if (StringUtils.isEmpty(consistencyTaskShardingContext.getChecksum())
                || !checksum.equals(consistencyTaskShardingContext.getChecksum())) {
                this.peerElectionHandler.setConsistencyTaskShardingContext(leaderToFollowerHeartbeatRequest);
            }
            return CommonRes.success(this.createResponse());
        } catch (Exception e) {
            return CommonRes.fail(new CommonError(EmBusinessError.SYS_INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * 创建心跳响应对象
     *
     * @return 响应
     */
    private LeaderToFollowerHeartbeatResponse createResponse() {
        return LeaderToFollowerHeartbeatResponse.builder().success(true)
            .responsePeerId(this.peerElectionHandler.getConsistencyTaskShardingContext().getCurrentPeerId())
            .lastResponseTs(System.currentTimeMillis()).build();
    }

}
