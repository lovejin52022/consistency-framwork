package com.zzjj.consistency.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * 节点集群配置信息
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@ConfigurationProperties(prefix = "consistency.peers")
public class PeerNodeConfigProperties {

    /**
     * 集群节点的配置信息 格式: ip1:port:id1,ip2:port:id2,ip3:port:id3
     */
    public String peersConfig;

}
