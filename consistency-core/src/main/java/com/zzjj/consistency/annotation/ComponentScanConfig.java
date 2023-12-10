package com.zzjj.consistency.annotation;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author zengjin
 * @date 2023/11/19
 **/
@Configuration
@ComponentScan(value = {"com.zzjj.consistency"})
@MapperScan(basePackages = {"com.zzjj.consistency.mapper"})
public class ComponentScanConfig {

}
