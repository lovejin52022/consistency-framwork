package com.zzjj.consistency.config;

import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.zzjj.consistency.storage.RocksLocalStorage;

/**
 * rocksdb配置
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Component
public class RocksDBConfig {
    @Autowired
    private ConsistencyConfiguration tendConsistencyConfiguration;

    @Bean
    public RocksLocalStorage rocksStore() throws RocksDBException {
        return new RocksLocalStorage(this.tendConsistencyConfiguration.getRocksPath());
    }
}
