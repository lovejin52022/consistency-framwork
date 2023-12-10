package com.zzjj.consistency.common;

import lombok.Getter;

/**
 * @author zengjin
 * @date 2023/11/19
 **/
@Getter
public enum EmBusinessError  {

    // 通用异常码
    SYS_INTERNAL_SERVER_ERROR(10006, "系统内部错误"),
    ;

    private Integer errCode;

    private String errMsg;

    EmBusinessError(Integer errCode, String errMsg) {
        this.errCode = errCode;
        this.errMsg  = errMsg;
    }

    public void setErrCode(Integer errCode) {
        this.errCode = errCode;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

}