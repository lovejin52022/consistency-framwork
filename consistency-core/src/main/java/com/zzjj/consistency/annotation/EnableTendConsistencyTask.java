package com.zzjj.consistency.annotation;

import java.lang.annotation.*;

import org.springframework.context.annotation.Import;

/**
 * 一致性任务注解
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.TYPE)
@Import({ConsistencyTaskSelector.class})
public @interface EnableTendConsistencyTask {}
