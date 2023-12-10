package com.zzjj.consistency.enums;

/**
 * 一致性任务状态
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public enum ConsistencyTaskStatusEnum {

    /**
     * 0:初始化 1:开始执行 2:执行失败 3:执行成功
     */
    INIT(0), START(1), FAIL(2), SUCCESS(3);

    private final Integer code;

    ConsistencyTaskStatusEnum(final int code) {
        this.code = code;
    }

    public Integer getCode() {
        return this.code;
    }
}
