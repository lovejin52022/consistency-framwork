package com.zzjj.consistency.demo.order.entity;

import java.io.Serializable;
import java.util.Date;

import lombok.Data;

/**
 * @author zengjin
 * @date 2023/12/10 12:33
 **/
@Data
public class OrderSnapshotDO implements Serializable {

    private static final long serialVersionUID = 7887722303588539577L;
    private Long id;

    /**
     * 创建时间
     */
    private Date gmtCreate;

    /**
     * 更新时间
     */
    private Date gmtModified;

    /**
     * 订单号
     */
    private String orderId;

    /**
     * 快照类型
     */
    private Integer snapshotType;

    /**
     * 订单快照内容
     */
    private String snapshotJson;

}
