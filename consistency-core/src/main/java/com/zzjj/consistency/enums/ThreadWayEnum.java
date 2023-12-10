package com.zzjj.consistency.enums;

/**
 * 线程调用方式
 *
 * @author zengjin
 * @date 2023/11/19
 **/
public enum ThreadWayEnum {

    /**
     * 异步执行
     */
    ASYNC(1, "异步执行"),
    /**
     * 同步执行
     */
    SYNC(2, "同步执行");

    private final Integer code;

    private final String desc;

    ThreadWayEnum(final int code, final String desc) {
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
