package com.zzjj.consistency.demo.order.dao;

import java.util.List;

import org.springframework.stereotype.Component;

import com.zzjj.consistency.annotation.ConsistencyTask;
import com.zzjj.consistency.demo.order.entity.OrderSnapshotDO;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zengjin
 * @date 2023/12/10 12:33
 **/
@Slf4j
@Component
public class OrderSnapshotDAO {

    /**
     * 批量插入操作
     *
     * @param orderSnapshotDOList 要插入快照的集合
     */
    @ConsistencyTask(alertActionBeanName = "tendConsistencyAlerter")
    public void batchSave(final List<OrderSnapshotDO> orderSnapshotDOList) {
        OrderSnapshotDAO.log.info("进入OrderSnapshotDAO#batchSave orderSnapshotDOList={}",
            JSONUtil.toJsonPrettyStr(orderSnapshotDOList));
    }

}
