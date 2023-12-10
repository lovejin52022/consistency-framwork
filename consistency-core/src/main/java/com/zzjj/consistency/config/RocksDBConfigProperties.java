package com.zzjj.consistency.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * rocksdb配置路径
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Data
@ConfigurationProperties(prefix = "consistency.rocksdb")
public class RocksDBConfigProperties {

    /**
     * RocksDB的存储文件夹目录
     */
    public String rocksPath;

}
