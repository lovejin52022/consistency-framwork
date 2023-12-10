package com.zzjj.consistency.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.zzjj.consistency.config.ConsistencyConfiguration;
import com.zzjj.consistency.election.PeerElectionHandler;
import com.zzjj.consistency.exceptions.ConsistencyException;
import com.zzjj.consistency.model.ConsistencyTaskInstance;
import com.zzjj.consistency.storage.RocksLocalStorage;
import com.zzjj.consistency.utils.ReflectTools;
import com.zzjj.consistency.utils.SpringBeanUtil;
import com.zzjj.consistency.utils.ThreadLocalUtil;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 任务调度管理器
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@Component
public class TaskScheduleManager {
    /**
     * 任务存储mapper组件
     */
    @Autowired
    private TaskStoreService taskStoreService;
    /**
     * 并行任务线程池
     */
    @Autowired
    private CompletionService<ConsistencyTaskInstance> consistencyTaskPool;

    @Resource
    private TaskEngineExecutor taskEngineExecutor;
    /**
     * 一致性任务分片组件
     */
    @Autowired
    private PeerElectionHandler peerElectionHandler;
    /**
     * 一致性框架配置
     */
    @Autowired
    private ConsistencyConfiguration consistencyConfiguration;
    /**
     * RocksDB工具类
     */
    @Autowired
    private RocksLocalStorage rocksLocalStorage;

    /**
     * 查询并执行未完成的一致性任务
     */
    public void performanceTask() throws InterruptedException {
        log.info("performanceTask...");

        // 如果分片结果为空，即leader还没有做分片 或者 leader还没有启动
        final Map<String, List<Long>> taskSharingResult =
            this.peerElectionHandler.getConsistencyTaskShardingContext().getTaskSharingResult();
        if (ObjectUtil.isEmpty(taskSharingResult)) {
            log.warn("leader尚未启动, 等待leader启动分片后，会下发各节点任务分片索引.");
            return;
        }

        // 获取当前节点分配到的分片索引
        final List<Long> myTaskShardIndexes = this.peerElectionHandler.getMyTaskShardIndexes();
        log.info("当前节点的shard index为 {}", myTaskShardIndexes);
        // 如果当前自己的分片索引为空
        if (CollectionUtil.isEmpty(myTaskShardIndexes)) {
            log.warn("leader尚未完成任务分片");
            return;
        }

        // 从我们的db，mysql里，通过sql语句，去做一个查询，查询你的未完成的任务
        // 一下子会把所有的未完成的任务，都给他去查询出来
        List<ConsistencyTaskInstance> consistencyTaskInstances = new ArrayList<>();
        try {
            // 从数据库中拿到所有未完成的任务
            consistencyTaskInstances = this.taskStoreService.listByUnFinishTask();
        } catch (final Exception e) {
            log.error("调度器从数据库中获取待执行任务时，发生异常 {}", e.getMessage());
        }

        // 从RocksDB中获取待执行的任务
        final List<ConsistencyTaskInstance> waitPerformanceTaskList = this.listWaitPerformanceTaskFromRocks();

        // 如果本地和数据库都没有数据，则退出
        if (CollectionUtils.isEmpty(consistencyTaskInstances) && CollectionUtil.isEmpty(waitPerformanceTaskList)) {
            return;
        }

        // 这里本地和数据库还不能合并，因为RocksDB中的数据只有本地有，不能进行分片执行，数据库中的才能进行分片执行
        // 刚开始的话，对db的任务，去进行过滤，根据你所属的分片进行过滤
        // 你的每个db里查出来的任务，你要去计算，这个任务是属于哪个分片的，如果那个分片是属于你负责的
        // 此时的话呢，就可以把那些分片对应的任务过滤筛选出来
        consistencyTaskInstances = this.filterBelongToCurrentPeerTasks(consistencyTaskInstances, myTaskShardIndexes);

        // 合并本地与数据库中的任务。到这里才能进行任务的合并，因为RocksDB是内嵌的基于本地磁盘的KV存储引擎，任务信息只有在本地有。
        waitPerformanceTaskList.addAll(consistencyTaskInstances);

        // 合并后如果还是为空，退出执行
        if (CollectionUtils.isEmpty(waitPerformanceTaskList)) {
            return;
        }

        final CountDownLatch latch = new CountDownLatch(waitPerformanceTaskList.size());
        // 你希望开多少线程，并发的执行你的任务，你可以自己去配置线程池里的线程数量
        for (final ConsistencyTaskInstance instance : waitPerformanceTaskList) {
            this.consistencyTaskPool.submit(() -> {
                try {
                    // 执行任务
                    this.taskEngineExecutor.executeTaskInstance(instance);
                    return instance;
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        log.info("[一致性任务框架] 执行完成");
    }

    /**
     * 过滤可以执行的任务 任务时间到了 并且 是当前实例所属的分片
     * 
     * @param consistencyTaskInstances 任务实例列表
     * @param myTaskShardIndexes 当前实例被分配到的分片索引
     * @return 可以被执行的任务的列表
     */
    private List<ConsistencyTaskInstance> filterBelongToCurrentPeerTasks(
        List<ConsistencyTaskInstance> consistencyTaskInstances, final List<Long> myTaskShardIndexes) {
        // 获取任务总分片数
        final Long shardingCount = this.consistencyConfiguration.getTaskShardingCount();
        // 判断一致性任务框架是否开启了分库模式：如果是分库模式则用shardKey来匹配任务分片逻辑，否则使用id匹配任务分片逻辑。
        final Boolean taskSharded = this.consistencyConfiguration.getTaskSharded();
        if (!CollectionUtil.isEmpty(consistencyTaskInstances)) {
            if (taskSharded) {
                // 过滤出需要被执行的任务
                consistencyTaskInstances =
                    consistencyTaskInstances.stream()
                        .filter(e -> e.getExecuteTime() - System.currentTimeMillis() <= 0
                            && myTaskShardIndexes.contains(e.getShardKey() % shardingCount))
                        .collect(Collectors.toList());
            } else {
                // 过滤出需要被执行的任务
                consistencyTaskInstances =
                    consistencyTaskInstances.stream().filter(e -> e.getExecuteTime() - System.currentTimeMillis() <= 0
                        && myTaskShardIndexes.contains(e.getId() % shardingCount)).collect(Collectors.toList());
            }
        }
        return consistencyTaskInstances;
    }

    /**
     * 从RocksDB中获取数据
     * 
     * @return 待执行任务列表
     */
    private List<ConsistencyTaskInstance> listWaitPerformanceTaskFromRocks() {
        List<ConsistencyTaskInstance> waitPerformanceTaskList = new ArrayList<>();
        // 获取RocksDB中的数据
        if (this.rocksLocalStorage.priorityQueue.size() > 0) {
            waitPerformanceTaskList = this.rocksLocalStorage.getTopN(100);
        }
        return waitPerformanceTaskList;
    }

    /**
     * 执行指定任务
     *
     * @param taskInstance 任务实例信息
     */
    public void performanceTask(final ConsistencyTaskInstance taskInstance) {
        // 获取方法签名 格式：类路径#方法名(参数1的类型,参数2的类型,...参数N的类型)
        final String methodSignName = taskInstance.getMethodSignName();
        // 获取方法所在的类
        final Class<?> clazz = this.getTaskMethodClass(methodSignName.split("#")[0]);
        if (ObjectUtils.isEmpty(clazz)) {
            return;
        }
        final Object bean = SpringBeanUtil.getBean(clazz);
        if (ObjectUtils.isEmpty(bean)) {
            return;
        }
        // 后面把methodName独立出一个字段
        final String methodName = taskInstance.getMethodName();
        // 获取参数类型的字符串字符串 多个用逗号分隔
        final String[] parameterTypes = StringUtils.isEmpty(taskInstance.getParameterTypes()) ? new String[] {}
            : taskInstance.getParameterTypes().split(",");
        // 构造参数类数组
        final Class<?>[] parameterClasses = ReflectTools.buildTypeClassArray(parameterTypes);
        // 获取目标方法
        final Method targetMethod = this.getTargetMethod(methodName, parameterClasses, clazz);
        if (ObjectUtils.isEmpty(targetMethod)) {
            return;
        }
        // 构造方法入参
        final Object[] args = ReflectTools.buildArgs(taskInstance.getTaskParameter(), parameterClasses);
        try {
            // 执行目标方法调用
            ThreadLocalUtil.setFlag(true);
            targetMethod.invoke(bean, args);
            ThreadLocalUtil.setFlag(false);
        } catch (final InvocationTargetException e) {
            log.error("调用目标方法时，发生异常", e);
            final Throwable target = e.getTargetException();
            throw new ConsistencyException((Exception)target);
        } catch (final Exception ex) {
            throw new ConsistencyException(ex);
        }
    }

    /**
     * 获取目标方法
     *
     * @param methodName 方法名称
     * @param parameterTypeClassArray 入参类数组
     * @param clazz 方法所在类的Class对象
     * @return 目标方法
     */
    private Method getTargetMethod(final String methodName, final Class<?>[] parameterTypeClassArray,
        final Class<?> clazz) {
        try {
            return clazz.getMethod(methodName, parameterTypeClassArray);
        } catch (final NoSuchMethodException e) {
            log.error("获取目标方法失败", e);
            return null;
        }
    }

    /**
     * 构造任务方法所在的类对象
     *
     * @param className 类名称
     * @return 类对象
     */
    private Class<?> getTaskMethodClass(final String className) {
        final Class<?> clazz;
        try {
            clazz = Class.forName(className);
            return clazz;
        } catch (final ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

}
