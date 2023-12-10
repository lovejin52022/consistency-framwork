package com.zzjj.consistency.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.zzjj.consistency.config.ConsistencyConfiguration;
import com.zzjj.consistency.custom.alerter.ConsistencyFrameworkAlerter;
import com.zzjj.consistency.enums.ConsistencyTaskStatusEnum;
import com.zzjj.consistency.exceptions.ConsistencyException;
import com.zzjj.consistency.model.ConsistencyTaskInstance;
import com.zzjj.consistency.storage.RocksLocalStorage;
import com.zzjj.consistency.utils.ExpressionUtils;
import com.zzjj.consistency.utils.ReflectTools;
import com.zzjj.consistency.utils.SpringBeanUtil;
import com.zzjj.consistency.utils.TimeUtils;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 任务执行引擎实现类
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@Service
public class TaskEngineExecutorImpl implements TaskEngineExecutor {

    private static final String MY_SQL_NOT_OPEN_ERROR = "Could not open JDBC Connection for transaction";

    /**
     * 一致性任务存储的service接口
     */
    @Autowired
    private TaskStoreService taskStoreService;
    /**
     * 任务引擎管理器
     */
    @Autowired
    private TaskScheduleManager taskScheduleManager;
    /**
     * 告警通知的线程池
     */
    @Autowired
    private ThreadPoolExecutor alertNoticePool;
    /**
     * 获取框架级配置
     */
    @Autowired
    private ConsistencyConfiguration consistencyConfig;
    /**
     * 事务模板
     */
    @Resource
    private TransactionTemplate transactionTemplate;
    /**
     * RocksDB工具类
     */
    @Autowired
    private RocksLocalStorage rocksLocalStorage;

    /**
     * 执行指定的任务实例 这里使用try catch 是因为需要将任务的错误信息也保存到任务表 正常情况下 不能进行try catch，不然事务是无法回滚的
     *
     * @param taskInstance 任务实例信息
     */
    @Override
    // @Transactional(rollbackFor = Exception.class)
    // 注意：在模拟本地存储时，发现，如下问题：
    // 一开始，数据库开启的情况下，启动应用后，此时关闭数据库，任务初始化的时候，可以存储到RocksDB中，
    // 但是在调度器调度执行任务的时候，因为加了@Transactional注解，spring会基于@Transactional注解的拦截器中，
    // 新建事务，这里不能加事务注解 需要使用手工开启注解的方式，来执行任务。
    public void executeTaskInstance(final ConsistencyTaskInstance taskInstance) {
        try {
            this.transactionTemplate.execute(transactionStatus -> {
                this.doExecuteTaskInstance(taskInstance);
                return true;
            });
        } catch (final Exception e) {
            this.doExecuteTaskInstance(taskInstance);
        }
    }

    private void doExecuteTaskInstance(final ConsistencyTaskInstance taskInstance) {

        boolean isOpenLocalStorageMode = false;
        try {
            // 如果id是空的，说明：任务在初始化的时候，就出现了MySQL故障。任务实例会落到本地RocksDB的KV存储中。
            if (ObjectUtil.isEmpty(taskInstance.getId())) {
                isOpenLocalStorageMode = true;
            }

            // 如果没有开启本地存储模式
            if (!isOpenLocalStorageMode) {
                // 启动任务 MySQL故障点1：如果这里数据库挂了，此时任务状态是 [初始化] 或者 [执行失败] 的状态，需要持久化到本地存储.
                this.taskStoreService.turnOnTask(taskInstance);
            }
            taskInstance.setTaskStatus(ConsistencyTaskStatusEnum.START.getCode());
            // 执行任务
            this.taskScheduleManager.performanceTask(taskInstance);
            // 如果执行成功，到了这里，就标记为执行成功，以防止，下面markSuccess的时候，出现数据库故障。
            // 这样在进入catch块的时候，还可以做下区分
            taskInstance.setTaskStatus(ConsistencyTaskStatusEnum.SUCCESS.getCode());
            if (!isOpenLocalStorageMode) {
                // MySQL故障点3：此时任务已经被标记为执行成功,这里会移除该任务。 如果说这里移除任务的时候，发现MySQL挂了，
                // 等数据库恢复后，会发生任务被重复执行，由业务服务的幂等保障机制来处理。
                final int successResult = this.taskStoreService.markSuccess(taskInstance);
                log.info("[一致性任务框架] 标记为执行成功的结果为 [{}]", successResult > 0);
            } else {
                // 从RocksDB中移除
                this.rocksRemove(taskInstance);
                log.info("rocksRemoveFallback删除key成功");
            }
        } catch (final Exception e) {
            log.error("[一致性任务框架] 执行一致性任务时发生异常, taskInstance的实例信息为 {}", JSONUtil.toJsonStr(taskInstance), e);
            // 不是数据库无法连接的异常
            if (!e.getMessage().contains(MY_SQL_NOT_OPEN_ERROR)) {
                taskInstance.setExecuteTime(this.getNextExecuteTime(taskInstance));
            }
            taskInstance.setErrorMsg(this.getErrorMsg(e));
            taskInstance.setTaskStatus(ConsistencyTaskStatusEnum.FAIL.getCode());
            try {
                this.taskStoreService.markFail(taskInstance);
            } catch (final Exception ex) {
                log.error("[一致性任务框架] 标记任务执行失败时，发生异常", e);
            }
            // 执行降级逻辑
            this.fallbackExecuteTask(taskInstance, isOpenLocalStorageMode, e);
        }
    }

    /**
     * 当执行任务失败的时候，执行该逻辑
     *
     * @param taskInstance 任务实例
     * @param isOpenLocalStorageMode 任务实例是否是本地存储模式
     */
    @Override
    public void fallbackExecuteTask(final ConsistencyTaskInstance taskInstance, final boolean isOpenLocalStorageMode,
        final Exception ex) {
        log.info("[一致性任务框架] 执行任务降级逻辑...");
        // 如果是数据库连不上的异常，那么就将数据存储到本地。
        // 这里用字符串匹配的方式，是因为框架本身，没有mysql驱动，因为本事也是要嵌入到业务服务中运行的，所以使用字符串匹配的方式
        if (ex.getMessage().contains(MY_SQL_NOT_OPEN_ERROR)) {
            // 将任务实例存储到RocksDB,有一致性框架内部的调度引擎，去再次执行该任务。
            this.rocksStore(taskInstance);
        }
        // 如果注解(任务实例信息)中没有提供降级类，则退出，不执行降级
        if (StringUtils.isEmpty(taskInstance.getFallbackClassName())
            || "void".equals(taskInstance.getFallbackClassName())) {
            // 解析并对表达式结果进行校验，并执行相关的告警通知逻辑
            // 如果没配置降级类且符合告警通知的表达式，则直接进行告警
            this.parseExpressionAndDoAlert(taskInstance);
            return;
        }
        // 获取全局配置 默认是开启降级策略的 如果失败会进行降级
        if (taskInstance.getExecuteTimes() <= this.consistencyConfig.getFailCountThreshold()) {
            return;
        }
        final Class<?> fallbackClass = ReflectTools.getClassByName(taskInstance.getFallbackClassName());
        if (ObjectUtils.isEmpty(fallbackClass)) {
            return;
        }
        // 获取参数值列表的json数组字符串
        final String taskParameterText = taskInstance.getTaskParameter();
        // 参数类型字符串 多个用逗号进行了分隔
        final String parameterTypes = taskInstance.getParameterTypes();
        // 构造参数类数组
        final Class<?>[] paramTypes = this.getParamTypes(parameterTypes);
        // 参数具体的值
        final Object[] paramValues = ReflectTools.buildArgs(taskParameterText, paramTypes);
        // 从spring容器中获取相关降级的bean
        final Object fallbackClassBean = this.getBeanBySpringApplicationContext(fallbackClass, paramValues);
        // 获取降级方法
        final Method fallbackMethod = ReflectUtil.getMethod(fallbackClass, taskInstance.getMethodName(), paramTypes);
        try {
            // 执行降级逻辑的方法
            fallbackMethod.invoke(fallbackClassBean, paramValues);
            if (!isOpenLocalStorageMode) {
                // 标记为执行成功 这里会移除该任务
                final int successResult = this.taskStoreService.markSuccess(taskInstance);
                log.info("[一致性任务框架] 降级逻辑执行成功 标记为执行成功的结果为 [{}]", successResult > 0);
            } else {
                this.rocksRemove(taskInstance);
            }
        } catch (final Exception e) {
            // 解析并对表达式结果进行校验，并执行相关的告警通知逻辑
            // 在执行完降级逻辑后，再去发送消息。因为如果降级成功了，也就不用发送告警通知了。如果降级失败，再去发送告警通知。
            this.parseExpressionAndDoAlert(taskInstance);
            taskInstance.setFallbackErrorMsg(this.getErrorMsg(e));
            if (!isOpenLocalStorageMode) {
                final int failResult = this.taskStoreService.markFallbackFail(taskInstance);
                log.error("[一致性任务框架] 降级逻辑也失败了 标记为执行失败的结果为 [{}] 下次调度时间为 [{} - {}]", failResult > 0,
                    taskInstance.getExecuteTime(), this.getFormatTime(taskInstance.getExecuteTime()), e);
            } else {
                this.rocksStore(taskInstance);
                log.error("[一致性任务框架] 降级逻辑也失败了, 在RockDB进行了记录，标记为执行失败，下次调度时间为 [{} - {}]", taskInstance.getExecuteTime(),
                    this.getFormatTime(taskInstance.getExecuteTime()), e);
            }
        }
    }

    /**
     * 从RocksDB中获取实例任务
     * 
     * @param taskInstance 实例任务
     */
    private String getTaskInstanceFromRocks(final ConsistencyTaskInstance taskInstance) {
        // 记录到RocksDB
        return this.rocksLocalStorage.get(taskInstance);
    }

    /**
     * 存储降级，将任务存储到RocksDB中
     * 
     * @param taskInstance 任务实例信息
     */
    private void rocksStore(final ConsistencyTaskInstance taskInstance) {
        if (StringUtils.isEmpty(this.getTaskInstanceFromRocks(taskInstance))) {
            // 记录到RocksDB
            this.rocksLocalStorage.put(taskInstance);
        }
    }

    /**
     * 从RocksDB中删除一个实例
     * 
     * @param taskInstance 任务实例信息
     */
    private void rocksRemove(final ConsistencyTaskInstance taskInstance) {
        if (!StringUtils.isEmpty(this.getTaskInstanceFromRocks(taskInstance))) {
            // 记录到RocksDB
            this.rocksLocalStorage.delete(taskInstance);
        }
    }

    /**
     * 从spring容器中获取相关降级的bean
     *
     * @param fallbackClass 降级的Class类对象
     * @param paramValues 参数值
     * @return 相关降级的bean
     */
    private Object getBeanBySpringApplicationContext(final Class<?> fallbackClass, final Object[] paramValues) {
        return SpringBeanUtil.getBean(fallbackClass, paramValues);
    }

    /**
     * 获取参数类型数组
     *
     * @param taskParameterText 参数类型json字符串 可转为JsonArray
     * @return 参数类型数组
     */
    private Class<?>[] getParamTypes(final String taskParameterText) {
        return ReflectTools.buildTypeClassArray(taskParameterText.split(","));
    }

    /**
     * 解析并对表达式结果进行校验，并执行相关的告警通知逻辑
     *
     * @param taskInstance 任务实例信息
     */
    private void parseExpressionAndDoAlert(final ConsistencyTaskInstance taskInstance) {
        try {
            if (StringUtils.isEmpty(taskInstance.getAlertExpression())) {
                return;
            }
            // 使用线程的原因是不对正常业务调用造成时间的占用 一般推送消息使用的是发送短信，钉钉、企业微信、邮件等等，
            // 操作会有一定的耗时（不过这个也要看具体的实现类是怎么实现的，如果实现类中使用的是异步推送告警，其实这里也就不用放到线程池中了）
            this.alertNoticePool.submit(() -> {
                // 对表达式进行重写
                final String expr = ExpressionUtils.rewriteExpr(taskInstance.getAlertExpression());
                // 获取表达式解析后的结果
                final String exprResult = ExpressionUtils.readExpr(expr, ExpressionUtils.buildDataMap(taskInstance));
                // 执行alert告警
                this.doAlert(exprResult, taskInstance);
            });
        } catch (final Exception e) {
            log.error("发送告警通知时，发生异常", e);
        }
    }

    /**
     * 执行告警
     *
     * @param exprResult 表达式解析后的结果
     * @param taskInstance 任务实例信息
     */
    private void doAlert(final String exprResult, final ConsistencyTaskInstance taskInstance) {
        if (StringUtils.isEmpty(exprResult)) {
            return;
        }
        if (!ExpressionUtils.RESULT_FLAG.equals(exprResult)) {
            return;
        }
        // 执行相关的动作告警动作 发送钉钉消息/发送短信/访问一个URL接口等等方式 这里暂时先打印一条告警日志来代替 如果业务服务实现了框架提供的接口，则会进行调用相关的告警通知逻辑
        log.warn("[一致性任务框架] 告警通知 实例id为{}的任务{}触发告警规则，请进行排查。", taskInstance.getId(),
            JSONUtil.toJsonPrettyStr(taskInstance));
        if (StringUtils.isEmpty(taskInstance.getAlertActionBeanName())) {
            return;
        }
        // 发送告警通知
        this.sendAlertNotice(taskInstance);
    }

    /**
     * 发送告警通知
     *
     * @param taskInstance 告警通知
     */
    private void sendAlertNotice(final ConsistencyTaskInstance taskInstance) {
        // 获取Spring容器中所有对于ConsistencyFrameworkAlerter接口的实现类
        final Map<String, ConsistencyFrameworkAlerter> beansOfTypeMap =
            SpringBeanUtil.getBeansOfType(ConsistencyFrameworkAlerter.class);

        if (CollectionUtils.isEmpty(beansOfTypeMap)) {
            log.warn("[一致性任务框架] 未获取到 ConsistencyFrameworkAlerter 相关的实现类，无法进行告警通知...");
            return;
        }

        try {
            // 获取ConsistencyFrameworkAlerter的实现类并发送告警通知
            this.getConsistencyFrameworkAlerterImpler(beansOfTypeMap, taskInstance).sendAlertNotice(taskInstance);
        } catch (final Exception e) {
            log.error("[一致性任务框架] 调用业务服务实现具体的告警通知类时，发生异常", e);
            throw new ConsistencyException(e);
        }
    }

    /**
     * 获取ConsistencyFrameworkAlerter的实现类
     *
     * @param beansOfTypeMap ConsistencyFrameworkAlerter接口实现类的map集合
     * @param taskInstance 任务实例信息
     * @return 获取ConsistencyFrameworkAlerter的实现类
     */
    private ConsistencyFrameworkAlerter getConsistencyFrameworkAlerterImpler(
        final Map<String, ConsistencyFrameworkAlerter> beansOfTypeMap, final ConsistencyTaskInstance taskInstance) {
        // 如果只有一个实现类
        if (beansOfTypeMap.size() == 1) {
            final String[] beanNamesForType = SpringBeanUtil.getBeanNamesForType(ConsistencyFrameworkAlerter.class);
            return (ConsistencyFrameworkAlerter)SpringBeanUtil.getBean(beanNamesForType[0]);
        }

        // 如果有多个实现类 获取注解中定义好的执行告警动作的alertActionBeanName获取对应的实现类
        return beansOfTypeMap.get(taskInstance.getAlertActionBeanName());
    }

    /**
     * 获取任务下一次的执行时间
     *
     * @param taskInstance 一致性任务实例
     * @return 下次执行时间
     */
    private long getNextExecuteTime(final ConsistencyTaskInstance taskInstance) {
        // 上次执行时间 + （下一次执行的次数 * 执行间隔）
        return taskInstance.getExecuteTime()
            + ((taskInstance.getExecuteTimes() + 1) * TimeUtils.secToMill(taskInstance.getExecuteIntervalSec()));
    }

    private String getFormatTime(final long timestamp) {
        // 设置格式
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(timestamp);
    }

    /**
     * 获取异常信息
     *
     * @param e 异常对象
     * @return 异常信息
     */
    private String getErrorMsg(final Exception e) {
        if ("".equals(e.getMessage())) {
            return "";
        }
        String errorMsg = e.getMessage();
        if (StringUtils.isEmpty(errorMsg)) {
            if (e instanceof IllegalAccessException) {
                final IllegalAccessException illegalAccessException = (IllegalAccessException)e;
                errorMsg = illegalAccessException.getMessage();
            } else if (e instanceof IllegalArgumentException) {
                final IllegalArgumentException illegalArgumentException = (IllegalArgumentException)e;
                errorMsg = illegalArgumentException.getMessage();
            } else if (e instanceof InvocationTargetException) {
                final InvocationTargetException invocationTargetException = (InvocationTargetException)e;
                errorMsg = invocationTargetException.getTargetException().getMessage();
            }
        }
        return errorMsg.substring(0, Math.min(errorMsg.length(), 200));
    }

}
