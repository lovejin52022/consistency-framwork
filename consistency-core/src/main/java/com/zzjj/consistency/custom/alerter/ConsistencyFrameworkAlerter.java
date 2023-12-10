package com.zzjj.consistency.custom.alerter;

import com.zzjj.consistency.model.ConsistencyTaskInstance;

/**
 * 一致性框架告警接口
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public interface ConsistencyFrameworkAlerter {

    /**
     * 发送告警通知 用户拿到告警实例通知给对应的人，在该方法中实现即可
     *
     * @param consistencyTaskInstance 发送告警通知
     */
    void sendAlertNotice(ConsistencyTaskInstance consistencyTaskInstance);

}
