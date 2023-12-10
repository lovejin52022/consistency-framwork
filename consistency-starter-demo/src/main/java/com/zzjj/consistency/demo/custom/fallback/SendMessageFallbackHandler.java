package com.zzjj.consistency.demo.custom.fallback;

import org.springframework.stereotype.Component;

import com.zzjj.consistency.demo.order.OrderInfoDTO;

import cn.hutool.json.JSONUtil;

/**
 * 自定义降级类
 * 
 * @author zengjin
 * @date 2023/12/10 12:27
 **/
@Component
public class SendMessageFallbackHandler {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SendMessageFallbackHandler.class);

    // 注：降级方法要与原方法的方法名，方法入参、返回值都完全一样

    public void send(final OrderInfoDTO orderInfo) {
        SendMessageFallbackHandler.LOG.info("触发send方法的降级逻辑...");
    }

    public void sendRightNowAsyncMessage(final OrderInfoDTO orderInfo) {
        SendMessageFallbackHandler.LOG.info("[立即执行异步任务测试] 降级逻辑 执行sendRightNowAsyncMessage(OrderInfoDTO)方法 {}",
            JSONUtil.toJsonStr(orderInfo));
        System.out.println(1 / 0); // 模拟降级也失败的情况
    }

}
