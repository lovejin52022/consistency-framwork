package com.zzjj.consistency.exceptions;

/**
 * 异常类
 *
 * @author zengjin
 * @date 2023/11/19
 **/
public class ConsistencyException extends RuntimeException {

    private static final long serialVersionUID = 8288212781358664864L;

    public ConsistencyException() {}

    public ConsistencyException(final Exception e) {
        super(e);
    }

    public ConsistencyException(final String message) {
        super(message);
    }

}
