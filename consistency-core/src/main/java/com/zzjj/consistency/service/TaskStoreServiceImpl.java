package com.zzjj.consistency.service;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StringUtils;

import com.zzjj.consistency.config.ConsistencyConfiguration;
import com.zzjj.consistency.custom.query.TaskTimeRangeQuery;
import com.zzjj.consistency.enums.ConsistencyTaskStatusEnum;
import com.zzjj.consistency.enums.ExecuteEnum;
import com.zzjj.consistency.enums.ThreadWayEnum;
import com.zzjj.consistency.exceptions.ConsistencyException;
import com.zzjj.consistency.mapper.TaskStoreMapper;
import com.zzjj.consistency.model.ConsistencyTaskInstance;
import com.zzjj.consistency.storage.RocksLocalStorage;
import com.zzjj.consistency.utils.ReflectTools;
import com.zzjj.consistency.utils.SpringBeanUtil;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 任务存储的service实现类
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@Service
public class TaskStoreServiceImpl implements TaskStoreService {

    /**
     * 任务存储的mapper组件
     */
    @Autowired
    private TaskStoreMapper taskStoreMapper;
    /**
     * 任务执行线程池
     */
    @Autowired
    private CompletionService<ConsistencyTaskInstance> consistencyTaskPool;
    /**
     * 一致性框架配置
     */
    @Autowired
    private ConsistencyConfiguration consistencyConfiguration;
    /**
     * 任务执行器
     */
    @Autowired
    private TaskEngineExecutor taskEngineExecutor;
    /**
     * RocksDB工具类
     */
    @Autowired
    private RocksLocalStorage rocksLocalStorage;

    /**
     * 初始化最终一致性任务实例到数据库
     *
     * @param taskInstance 要存储的最终一致性任务的实例信息
     */
    @Override
    public void initTask(final ConsistencyTaskInstance taskInstance) {
        Long result = null;
        // 如果写数据到MySQL失败了，那么可以将数据加入到RocksDB
        try {
            result = this.taskStoreMapper.initTask(taskInstance);
            TaskStoreServiceImpl.log.info("[一致性任务框架] 初始化任务结果为 [{}]", result > 0);
        } catch (final Exception e) {
            TaskStoreServiceImpl.log.error("[一致性任务框架] 初始化任务到数据库时，发生异常，执行降级逻辑，将任务持久化到RocksDB本地存储中, 任务信息为 {}",
                JSONUtil.toJsonStr(taskInstance), e);
            // 将数据存储到RocksDB中
            this.rocksLocalStorage.put(taskInstance);
        }
        // 如果执行模式不是立即执行的任务, 调度任务执行
        if (!ExecuteEnum.EXECUTE_RIGHT_NOW.getCode().equals(taskInstance.getExecuteWay())) {
            return;
        }

        // 判断当前Action是否包含在事务里面，如果是，等事务提交后，再执行Action
        final boolean synchronizationActive = TransactionSynchronizationManager.isSynchronizationActive();
        if (synchronizationActive) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    TaskStoreServiceImpl.this.submitTaskInstance(taskInstance);
                }
            });
        } else {
            this.submitTaskInstance(taskInstance);
        }
    }

    /**
     * 根据id获取任务实例信息
     *
     * @param id 任务id
     * @param shardKey 任务分片键
     * @return 任务实例信息
     */
    @Override
    public ConsistencyTaskInstance getTaskByIdAndShardKey(final Long id, final Long shardKey) {
        return this.taskStoreMapper.getTaskByIdAndShardKey(id, shardKey);
    }

    /**
     * 获取未完成的任务
     *
     * @return 未完成任务的结果集
     */
    @Override
    public List<ConsistencyTaskInstance> listByUnFinishTask() {
        final Date startTime;
        final Date endTime;
        final Long limitTaskCount;
        try {
            // 获取TaskTimeLineQuery实现类
            if (!StringUtils.isEmpty(this.consistencyConfiguration.getTaskScheduleTimeRangeClassName())) {
                // 获取Spring容器中所有对于TaskTimeRangeQuery接口的实现类
                final Map<String, TaskTimeRangeQuery> beansOfTypeMap =
                    SpringBeanUtil.getBeansOfType(TaskTimeRangeQuery.class);
                final TaskTimeRangeQuery taskTimeRangeQuery = this.getTaskTimeLineQuery(beansOfTypeMap);
                startTime = taskTimeRangeQuery.getStartTime();
                endTime = taskTimeRangeQuery.getEndTime();
                limitTaskCount = taskTimeRangeQuery.limitTaskCount();
                return this.taskStoreMapper.listByUnFinishTask(startTime.getTime(), endTime.getTime(), limitTaskCount);
            } else {
                startTime = TaskTimeRangeQuery.defaultGetStartTime();
                endTime = TaskTimeRangeQuery.defaultGetEndTime();
                limitTaskCount = TaskTimeRangeQuery.defaultLimitTaskCount();
            }
        } catch (final Exception e) {
            TaskStoreServiceImpl.log.error("[一致性任务框架] 调用业务服务实现具体的告警通知类时，发生异常", e);
            throw new ConsistencyException(e);
        }
        return this.taskStoreMapper.listByUnFinishTask(startTime.getTime(), endTime.getTime(), limitTaskCount);
    }

    /**
     * 获取TaskTimeRangeQuery的实现类
     *
     * @param beansOfTypeMap TaskTimeRangeQuery接口实现类的map集合
     * @return 获取TaskTimeRangeQuery的实现类
     */
    private TaskTimeRangeQuery getTaskTimeLineQuery(final Map<String, TaskTimeRangeQuery> beansOfTypeMap) {
        // 如果只有一个实现类
        if (beansOfTypeMap.size() == 1) {
            final String[] beanNamesForType = SpringBeanUtil.getBeanNamesForType(TaskTimeRangeQuery.class);
            return (TaskTimeRangeQuery)SpringBeanUtil.getBean(beanNamesForType[0]);
        }

        final Class<?> clazz =
            ReflectTools.getClassByName(this.consistencyConfiguration.getTaskScheduleTimeRangeClassName());
        return (TaskTimeRangeQuery)SpringBeanUtil.getBean(clazz);
    }

    /**
     * 启动任务
     *
     * @param consistencyTaskInstance 任务实例信息
     * @return 启动任务的结果
     */
    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRES_NEW)
    @Override
    public int turnOnTask(final ConsistencyTaskInstance consistencyTaskInstance) {
        consistencyTaskInstance.setExecuteTime(System.currentTimeMillis());
        consistencyTaskInstance.setTaskStatus(ConsistencyTaskStatusEnum.START.getCode());
        return this.taskStoreMapper.turnOnTask(consistencyTaskInstance);
    }

    /**
     * 标记任务成功
     *
     * @param consistencyTaskInstance 任务实例信息
     * @return 标记结果
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public int markSuccess(final ConsistencyTaskInstance consistencyTaskInstance) {
        return this.taskStoreMapper.markSuccess(consistencyTaskInstance);
    }

    /**
     * 标记任务为失败
     *
     * @param consistencyTaskInstance 一致性任务信息
     * @return 标记结果
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public int markFail(final ConsistencyTaskInstance consistencyTaskInstance) {
        return this.taskStoreMapper.markFail(consistencyTaskInstance);
    }

    /**
     * 标记为降级失败
     *
     * @param consistencyTaskInstance 一致性任务实例
     * @return 标记结果
     */
    @Override
    public int markFallbackFail(final ConsistencyTaskInstance consistencyTaskInstance) {
        return this.taskStoreMapper.markFallbackFail(consistencyTaskInstance);
    }

    /**
     * 提交任务
     *
     * @param taskInstance 任务实例
     */
    @Override
    public void submitTaskInstance(final ConsistencyTaskInstance taskInstance) {
        if (ThreadWayEnum.SYNC.getCode().equals(taskInstance.getThreadWay())) {
            // 选择事务事务模型并执行任务
            this.taskEngineExecutor.executeTaskInstance(taskInstance);
        } else if (ThreadWayEnum.ASYNC.getCode().equals(taskInstance.getThreadWay())) {
            this.consistencyTaskPool.submit(() -> {
                this.taskEngineExecutor.executeTaskInstance(taskInstance);
                return taskInstance;
            });
        }
    }

}
