package com.zzjj.consistency.enums;

/**
 * 节点状态
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public enum PeerOpTypeEnum {

    /**
     * 节点上线
     */
    ONLINE(1),
    /**
     * 节点下线
     */
    OFFLINE(2),;

    private final Integer opType;

    PeerOpTypeEnum(final Integer opType) {
        this.opType = opType;
    }

    public Integer getOpType() {
        return this.opType;
    }

}
