package com.zzjj.consistency.utils;

import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * 默认值获取工具
 *
 * @author zengjin
 * @date 2023/11/19
 **/
public class DefaultValueUtils {

    /**
     * 获取参数的值
     *
     * @param value        给定的值
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static String getOrDefault(String value, String defaultValue) {
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

    /**
     * 获取参数的值
     *
     * @param value        给定的值
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static Integer getOrDefault(Integer value, Integer defaultValue) {
        if (ObjectUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

    /**
     * 获取参数的值
     *
     * @param value        给定的值
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static Long getOrDefault(Long value, Long defaultValue) {
        if (ObjectUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }

    /**
     * 获取参数的值
     *
     * @param value        给定的值
     * @param defaultValue 默认值
     * @return 参数值
     */
    public static Boolean getOrDefault(Boolean value, Boolean defaultValue) {
        if (ObjectUtils.isEmpty(value)) {
            return defaultValue;
        }
        return value;
    }


}
