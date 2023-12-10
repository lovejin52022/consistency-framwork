package com.zzjj.consistency.service;

import com.zzjj.consistency.model.ConsistencyTaskInstance;

/**
 * 任务执行引擎接口
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public interface TaskEngineExecutor {

    /**
     * 执行指定的任务实例
     *
     * @param taskInstance 任务实例信息
     */
    void executeTaskInstance(ConsistencyTaskInstance taskInstance);

    /**
     * 当执行任务失败的时候，执行该逻辑
     * 
     * @param taskInstance 任务实例
     * @param isOpenLocalStorageMode 任务是否只在本地存储中进行了存储
     * @param ex 异常信息
     */
    void fallbackExecuteTask(ConsistencyTaskInstance taskInstance, boolean isOpenLocalStorageMode, Exception ex);

}
