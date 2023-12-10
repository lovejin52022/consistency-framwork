package com.zzjj.consistency.common;

import java.io.Serializable;

import lombok.Getter;

/**
 * 通用返回
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Getter
public class CommonRes<T> implements Serializable {

    private static final long serialVersionUID = 7081945253842986989L;
    private Boolean success;

    private T data;

    public CommonRes() {}

    public CommonRes(Boolean success) {
        this.success = success;
    }

    public CommonRes(Boolean success, T data) {
        this.success = success;
        this.data = data;
    }

    public static <T> CommonRes<T> success(T data) {
        return new CommonRes<>(true, data);
    }

    public static <CommonError> CommonRes<CommonError> fail(CommonError data) {
        return new CommonRes<>(false, data);
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public void setData(T data) {
        this.data = data;
    }
}
