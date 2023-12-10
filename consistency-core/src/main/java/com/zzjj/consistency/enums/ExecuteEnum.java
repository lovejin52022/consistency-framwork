package com.zzjj.consistency.enums;

/**
 * 执行模式枚举
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public enum ExecuteEnum {

    /**
     * 立即执行
     */
    EXECUTE_RIGHT_NOW(1, "立即执行"),
    /**
     * 调度执行
     */
    EXECUTE_SCHEDULE(2, "调度执行");

    private final Integer code;

    private final String desc;

    ExecuteEnum(final int code, final String desc) {
        this.code = code;
        this.desc = desc;
    }

    public Integer getCode() {
        return this.code;
    }

    public String getDesc() {
        return this.desc;
    }

}
