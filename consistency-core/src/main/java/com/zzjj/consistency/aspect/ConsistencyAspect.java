package com.zzjj.consistency.aspect;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.zzjj.consistency.annotation.ConsistencyTask;
import com.zzjj.consistency.config.ConsistencyConfiguration;
import com.zzjj.consistency.custom.shard.SnowflakeShardingKeyGenerator;
import com.zzjj.consistency.election.PeerElectionHandler;
import com.zzjj.consistency.enums.ConsistencyTaskStatusEnum;
import com.zzjj.consistency.enums.ExecuteEnum;
import com.zzjj.consistency.model.ConsistencyTaskInstance;
import com.zzjj.consistency.service.TaskStoreService;
import com.zzjj.consistency.utils.ReflectTools;
import com.zzjj.consistency.utils.ThreadLocalUtil;
import com.zzjj.consistency.utils.TimeUtils;

import cn.hutool.core.util.ReflectUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@Aspect
@Component
public class ConsistencyAspect {
    /**
     * 缓存生成任务分片key的对象实例
     */
    private static Object cacheGenerateShardKeyClassInstance = null;
    /**
     * 缓存生成任务分片key的方法
     */
    private static Method cacheGenerateShardKeyMethod = null;
    /**
     * 雪花算法workId
     */
    private static String workId;

    /**
     * 一致性任务的service
     */
    @Autowired
    private TaskStoreService taskStoreService;
    /**
     * 框架配置
     */
    @Autowired
    private ConsistencyConfiguration consistencyConfiguration;
    /**
     * 选举处理器
     */
    @Autowired
    private PeerElectionHandler peerElectionHandler;

    /**
     * 标注了ConsistencyTask的注解的方法执行前要做的工作
     *
     * @param point 切面信息
     */
    @Around("@annotation(consistencyTask)")
    public Object markConsistencyTask(final ProceedingJoinPoint point, final ConsistencyTask consistencyTask)
        throws Throwable {
        ConsistencyAspect.log.info("access method:{} is called on {} args {}", point.getSignature().getName(),
            point.getThis(), point.getArgs());

        // 是否是调度器在执行任务，如果是则直接执行任务即可，因为之前已经进行了任务持久化
        if (ThreadLocalUtil.getFlag()) {
            return point.proceed();
        }

        // 根据注解构造构造最终一致性任务的实例
        final ConsistencyTaskInstance taskInstance = this.createTaskInstance(consistencyTask, point);

        // 初始化任务数据到数据库
        this.taskStoreService.initTask(taskInstance);

        // 无论是调度执行还是立即执行的任务，任务初始化完成后不对目标方法进行访问，因此返回null
        return null;
    }

    /**
     * 根据注解构造构造最终一致性任务的实例
     *
     * @param task 一致性任务注解信息 相当于任务的模板
     * @param point 方法切入点
     * @return 一致性任务实例
     */
    private ConsistencyTaskInstance createTaskInstance(final ConsistencyTask task, final JoinPoint point) {
        // 根据入参数组获取对应的Class对象的数组
        final Class<?>[] argsClazz = ReflectTools.getArgsClass(point.getArgs());
        // 获取被拦截方法的全限定名称 格式：类路径#方法名(参数1的类型,参数2的类型,...参数N的类型)
        final String fullyQualifiedName = ReflectTools.getTargetMethodFullyQualifiedName(point, argsClazz);
        // 获取入参的类名称数组
        final String parameterTypes = ReflectTools.getArgsClassNames(point.getSignature());

        final Date date = new Date();
        final ConsistencyTaskInstance instance =
            ConsistencyTaskInstance.builder().taskId(StringUtils.isEmpty(task.id()) ? fullyQualifiedName : task.id())
                .methodName(point.getSignature().getName()).parameterTypes(parameterTypes)
                .methodSignName(fullyQualifiedName).taskParameter(JSONUtil.toJsonStr(point.getArgs()))
                .executeWay(task.executeWay().getCode()).threadWay(task.threadWay().getCode())
                .executeIntervalSec(task.executeIntervalSec()).delayTime(task.delayTime()).executeTimes(0)
                .taskStatus(ConsistencyTaskStatusEnum.INIT.getCode()).errorMsg("")
                .alertExpression(StringUtils.isEmpty(task.alertExpression()) ? "" : task.alertExpression())
                .alertActionBeanName(StringUtils.isEmpty(task.alertActionBeanName()) ? "" : task.alertActionBeanName())
                .fallbackClassName(ReflectTools.getFullyQualifiedClassName(task.fallbackClass())).fallbackErrorMsg("")
                .gmtCreate(date).gmtModified(date).build();
        // 设置执行时间
        instance.setExecuteTime(this.getExecuteTime(instance));
        // 设置分片key
        instance.setShardKey(this.consistencyConfiguration.getTaskSharded() ? this.generateShardKey() : 0L);

        return instance;
    }

    /**
     * 获取任务执行时间
     *
     * @param taskInstance 一致性任务实例
     * @return 下次执行时间
     */
    private long getExecuteTime(final ConsistencyTaskInstance taskInstance) {
        if (ExecuteEnum.EXECUTE_SCHEDULE.getCode().equals(taskInstance.getExecuteWay())) {
            final long delayTimeMillSecond = TimeUtils.secToMill(taskInstance.getDelayTime());
            return System.currentTimeMillis() + delayTimeMillSecond;
        } else {
            return System.currentTimeMillis();
        }
    }

    /**
     * 获取分片键
     *
     * @return 生成分片键
     */
    private Long generateShardKey() {
        final SnowflakeShardingKeyGenerator instance = SnowflakeShardingKeyGenerator.getInstance();
        if (!ObjectUtils.isEmpty(this.peerElectionHandler.getConsistencyTaskShardingContext())
            && StringUtils.isEmpty(ConsistencyAspect.workId)) {
            if (!ObjectUtils.isEmpty(this.peerElectionHandler.getConsistencyTaskShardingContext().getCurrentPeerId())) {
                ConsistencyAspect.workId =
                    this.peerElectionHandler.getConsistencyTaskShardingContext().getCurrentPeerId();
                instance.setWorkerId(ConsistencyAspect.workId);
            }
        }

        // 如果配置文件中，没有配置自定义任务分片键生成类，则使用框架自带的
        if (StringUtils.isEmpty(this.consistencyConfiguration.getShardingKeyGeneratorClassName())) {
            return instance.generateShardKey();
        }
        // 如果生成任务CACHE_GENERATE_SHARD_KEY_METHOD的方法存在，就直接调用该方法
        if (!ObjectUtils.isEmpty(ConsistencyAspect.cacheGenerateShardKeyMethod)
            && !ObjectUtils.isEmpty(ConsistencyAspect.cacheGenerateShardKeyClassInstance)) {
            try {
                return (Long)ConsistencyAspect.cacheGenerateShardKeyMethod
                    .invoke(ConsistencyAspect.cacheGenerateShardKeyClassInstance);
            } catch (final IllegalAccessException | InvocationTargetException e) {
                ConsistencyAspect.log.error("使用自定义类生成任务分片键时，发生异常", e);
            }
        }
        // 获取用户自定义的任务分片键的class
        final Class<?> shardingKeyGeneratorClass = this.getUserCustomShardingKeyGenerator();
        if (!ObjectUtils.isEmpty(shardingKeyGeneratorClass)) {
            final String methodName = "generateShardKey";
            final Method generateShardKeyMethod = ReflectUtil.getMethod(shardingKeyGeneratorClass, methodName);
            try {
                final Constructor<?> constructor = ReflectUtil.getConstructor(shardingKeyGeneratorClass);
                ConsistencyAspect.cacheGenerateShardKeyClassInstance = constructor.newInstance();
                ConsistencyAspect.cacheGenerateShardKeyMethod = generateShardKeyMethod;
                return (Long)ConsistencyAspect.cacheGenerateShardKeyMethod
                    .invoke(ConsistencyAspect.cacheGenerateShardKeyClassInstance);
            } catch (final IllegalAccessException | InvocationTargetException | InstantiationException e) {
                ConsistencyAspect.log.error("使用自定义类生成任务分片键时，发生异常", e);
                // 如果指定的自定义分片键生成报错，使用框架自带的
                return instance.generateShardKey();
            }
        }
        return instance.generateShardKey();
    }

    /**
     * 获取ShardingKeyGenerator的实现类
     */
    private Class<?> getUserCustomShardingKeyGenerator() {
        return ReflectTools.getClassByName(this.consistencyConfiguration.getShardingKeyGeneratorClassName());
    }

}
