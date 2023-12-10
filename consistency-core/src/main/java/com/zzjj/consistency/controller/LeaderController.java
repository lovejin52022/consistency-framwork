package com.zzjj.consistency.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.zzjj.consistency.common.CommonRes;
import com.zzjj.consistency.controller.data.FollowerToLeaderHeartbeatRequest;
import com.zzjj.consistency.controller.data.FollowerToLeaderHeartbeatResponse;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@RestController
@RequestMapping("/leader")
public class LeaderController {

    @PostMapping("/heartbeat")
    public CommonRes<?>
        followerHeartbeat(@RequestBody FollowerToLeaderHeartbeatRequest followerToLeaderHeartbeatRequest) {
        log.info("leader收到心跳请求 {}", JSONUtil.toJsonStr(followerToLeaderHeartbeatRequest));

        return CommonRes.success(this.createFollowerHeartbeatResponse());
    }

    private FollowerToLeaderHeartbeatResponse createFollowerHeartbeatResponse() {
        return FollowerToLeaderHeartbeatResponse.builder().success(true).replyTimestamp(System.currentTimeMillis())
            .build();
    }

}
