package com.zzjj.consistency.annotation;

import java.lang.annotation.*;

import com.zzjj.consistency.enums.ExecuteEnum;
import com.zzjj.consistency.enums.ThreadWayEnum;

/**
 * 一致性任务注解
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.METHOD)
public @interface ConsistencyTask {

    /**
     * 任务名称
     *
     * @return 任务名称
     */
    String id() default "";

    /**
     * 执行间隔默认20s
     *
     * @return 执行时间间隔
     */
    int executeIntervalSec() default 60;

    /**
     * 初始化延迟时间 单位秒
     *
     * @return 执行任务的延迟时间
     */
    int delayTime() default 60;

    /**
     * 告警表达式 如果满足告警表达式 会执行相关操作
     *
     * @return 告警表达式
     */
    String alertExpression() default "executeTimes > 1 && executeTimes < 5";

    /**
     * 告警的动作执行实现类的beanName 需要实现 ConsistencyFrameworkAlerter方法 并注入spring容器
     *
     * @return 告警的动作执行实现类的beanName
     */
    String alertActionBeanName() default "";

    /**
     * 降级方法的Class类 注：需要自定义的降级类中，实现与被注解的方法一样的实现
     *
     * @return 降级方法的Class类
     */
    Class<?> fallbackClass() default void.class;

    /**
     * 执行模式
     *
     * @return 执行模式
     */
    ExecuteEnum executeWay() default ExecuteEnum.EXECUTE_RIGHT_NOW;

    /**
     * 线程模型
     *
     * @return 线程模型
     */
    ThreadWayEnum threadWay() default ThreadWayEnum.ASYNC;
}
