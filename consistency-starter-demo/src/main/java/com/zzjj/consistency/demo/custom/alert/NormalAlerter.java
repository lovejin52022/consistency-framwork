package com.zzjj.consistency.demo.custom.alert;

import com.zzjj.consistency.custom.alerter.ConsistencyFrameworkAlerter;
import com.zzjj.consistency.model.ConsistencyTaskInstance;

/**
 * 自定义告警通知类 需要实现ConsistencyFrameworkAlerter类
 * 
 * @author zengjin
 * @date 2023/12/10 12:26
 **/
public class NormalAlerter implements ConsistencyFrameworkAlerter {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(NormalAlerter.class);

    /**
     * 发送告警通知 拿到告警实例通知给对应的人 要通知的人 在该方法中实现即可
     *
     * @param consistencyTaskInstance 发送告警通知
     */
    @Override
    public void sendAlertNotice(final ConsistencyTaskInstance consistencyTaskInstance) {
        NormalAlerter.LOG.info("执行告警通知逻辑... 方法签名为：{}", consistencyTaskInstance.getMethodSignName());
    }

}
