package com.zzjj.consistency.demo.order;

import java.util.List;

import org.springframework.stereotype.Component;

import com.zzjj.consistency.annotation.ConsistencyTask;
import com.zzjj.consistency.demo.custom.fallback.SendMessageFallbackHandler;
import com.zzjj.consistency.enums.ExecuteEnum;
import com.zzjj.consistency.enums.ThreadWayEnum;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zengjin
 * @date 2023/12/10 12:35
 **/
@Slf4j
@Component
public class SendMessageComponent {

    /**
     * 正常运行失败，降级运行成功 异步调度任务测试
     * <p>
     * 验证情况： 1、发送消息时，执行失败 有异常发生的情况，会标记任务状态为失败，同时记录失败的原因 2、当满足 降级条件(executeTimes(执行次数) >
     * TendConsistencyConfig.fallbackThreshold (默认值为0)) 可以触发降级逻辑 调用相关用户实现的自定义降级类的指定方法 3、当满足 默认的
     * alertExpression(executeTimes > 1 && executeTimes < 5) 告警通知时，会触发消息的推送，并可以调用相关实现类
     *
     * @param orderInfo 订单
     */
    @ConsistencyTask(executeIntervalSec = 20, delayTime = 10, executeWay = ExecuteEnum.EXECUTE_SCHEDULE,
        threadWay = ThreadWayEnum.ASYNC, fallbackClass = SendMessageFallbackHandler.class,
        alertActionBeanName = "normalAlerter")
    public void send(final OrderInfoDTO orderInfo) {
        // System.out.println(1 / 0); // 模拟失败
        SendMessageComponent.log.info("[异步调度任务测试] 执行send(OrderInfoDTO)方法 {}", JSONUtil.toJsonStr(orderInfo));
    }

    /**
     * 正常运行失败，降级也失败，触发告警通知
     * <p>
     * 立即执行 异步任务测试 立即执行异步任务的情况下 executeIntervalSec 和 delayTime 属性无用 开一个新的线程去执行任务
     * <p>
     * 验证情况： 1、发送消息时，执行失败 有异常发生的情况，会标记任务状态为失败，同时记录失败的原因 2、当满足 降级条件(executeTimes(执行次数) >
     * TendConsistencyConfig.fallbackThreshold (默认值为0)) 可以触发降级逻辑 调用相关用户实现的自定义降级类的指定方法 3、当满足 默认的
     * alertExpression(executeTimes > 1 && executeTimes < 5) 告警通知时，会触发消息的推送，并可以调用相关实现类
     *
     * @param orderInfo 订单
     */
    @ConsistencyTask(executeWay = ExecuteEnum.EXECUTE_RIGHT_NOW, threadWay = ThreadWayEnum.SYNC)
    public void sendRightNowAsyncMessage(final OrderInfoDTO orderInfo) {
        SendMessageComponent.log.info("[异步调度任务测试] 执行sendRightNowAsyncMessage(OrderInfoDTO)方法 {}", JSONUtil.toJsonStr(orderInfo));
        System.out.println(1 / 0); // 模拟失败
    }

    @ConsistencyTask(executeWay = ExecuteEnum.EXECUTE_RIGHT_NOW)
    public void sendRightNowAsyncMessage(final List<OrderInfoDTO> orderInfos) {
        SendMessageComponent.log.info("[异步调度任务测试] 执行sendRightNowAsyncMessage1(OrderInfoDTO)方法 {}", JSONUtil.toJsonStr(orderInfos));
        // System.out.println(1 / 0);
    }

    @ConsistencyTask(executeWay = ExecuteEnum.EXECUTE_RIGHT_NOW)
    public void sendRightNowAsyncMessage2() {
        SendMessageComponent.log.info("[异步调度任务测试] 执行sendRightNowAsyncMessage2(OrderInfoDTO)方法");
    }

}
