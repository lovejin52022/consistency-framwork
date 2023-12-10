package com.zzjj.consistency.config;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.zzjj.consistency.custom.query.TaskTimeRangeQuery;
import com.zzjj.consistency.custom.shard.ShardingKeyGenerator;
import com.zzjj.consistency.exceptions.ConsistencyException;
import com.zzjj.consistency.utils.DefaultValueUtils;
import com.zzjj.consistency.utils.ReflectTools;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 一致性框架配置装配类
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
@Configuration
@EnableConfigurationProperties(value = {ConsistencyParallelTaskConfigProperties.class,
    ConsistencyFallbackConfigProperties.class, SchedulerConfigProperties.class, ShardModeConfigProperties.class,
    PeerNodeConfigProperties.class, RocksDBConfigProperties.class})
public class ConsistencyAutoConfiguration {
    /**
     * 执行调度任务的线程池的配置
     */
    @Autowired
    private ConsistencyParallelTaskConfigProperties consistencyParallelTaskConfigProperties;
    /**
     * 降级逻辑相关参数配置
     */
    @Autowired
    private ConsistencyFallbackConfigProperties tendConsistencyFallbackConfigProperties;
    /**
     * 数据库分片模式参数配置
     */
    @Autowired
    private ShardModeConfigProperties shardModeConfigProperties;
    /**
     * 一致性任务节点的配置信息
     */
    @Autowired
    private PeerNodeConfigProperties peerNodeConfigProperties;
    /**
     * RocksDB的配置信息
     */
    @Autowired
    private RocksDBConfigProperties rocksDBConfigProperties;
    /**
     * 调度器相关的配置
     */
    @Autowired
    private SchedulerConfigProperties schedulerConfigProperties;

    /**
     *
     * 框架级配置 spring boot自动装配，就是说去收集各种各样的参数，把参数注入到properties里去 把各个properties里面的配置和信息，注入到 configuration里面去
     * 拿到一个完整的框架的配置，后续你的各种组件如果需要使用到框架的配置，就可以直接用这个configuration就可以了
     *
     * @return 配置bean
     */
    @Bean
    public ConsistencyConfiguration tendConsistencyConfigService() {
        // 对配置进行检查
        this.doConfigCheck(this.consistencyParallelTaskConfigProperties, this.shardModeConfigProperties,
            this.rocksDBConfigProperties, this.peerNodeConfigProperties);

        return ConsistencyConfiguration.builder()
            .threadCorePoolSize(
                DefaultValueUtils.getOrDefault(this.consistencyParallelTaskConfigProperties.getThreadCorePoolSize(), 5))
            .threadMaxPoolSize(
                DefaultValueUtils.getOrDefault(this.consistencyParallelTaskConfigProperties.getThreadMaxPoolSize(), 5))
            .threadPoolQueueSize(DefaultValueUtils
                .getOrDefault(this.consistencyParallelTaskConfigProperties.getThreadPoolQueueSize(), 100))
            .threadPoolKeepAliveTime(DefaultValueUtils
                .getOrDefault(this.consistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTime(), 60L))
            .threadPoolKeepAliveTimeUnit(DefaultValueUtils
                .getOrDefault(this.consistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTimeUnit(), "SECONDS"))
            .taskScheduleTimeRangeClassName(DefaultValueUtils
                .getOrDefault(this.consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName(), ""))
            .failCountThreshold(
                DefaultValueUtils.getOrDefault(this.tendConsistencyFallbackConfigProperties.getFailCountThreshold(), 2))
            .taskSharded(DefaultValueUtils.getOrDefault(this.shardModeConfigProperties.getTaskSharded(), false))
            .shardingKeyGeneratorClassName(
                DefaultValueUtils.getOrDefault(this.shardModeConfigProperties.getShardingKeyGeneratorClassName(), ""))
            .peersConfig(this.peerNodeConfigProperties.getPeersConfig())
            .taskShardingCount(this.getTaskShardingCountOrDefault())
            .rocksPath(this.getOrCreate(this.rocksDBConfigProperties.rocksPath))
            .consistencyTaskExecuteIntervalSeconds(DefaultValueUtils
                .getOrDefault(this.schedulerConfigProperties.getConsistencyTaskExecuteIntervalSeconds(), 10))
            .followerAliveCheckIntervalSeconds(DefaultValueUtils
                .getOrDefault(this.schedulerConfigProperties.getFollowerAliveCheckIntervalSeconds(), 10))
            .followerHeartbeatIntervalSeconds(DefaultValueUtils
                .getOrDefault(this.schedulerConfigProperties.getFollowerHeartbeatIntervalSeconds(), 10))
            .leaderAliveCheckIntervalSeconds(
                DefaultValueUtils.getOrDefault(this.schedulerConfigProperties.getLeaderAliveCheckIntervalSeconds(), 10))
            .leaderToFollowerHeartbeatIntervalSeconds(DefaultValueUtils
                .getOrDefault(this.schedulerConfigProperties.getLeaderToFollowerHeartbeatIntervalSeconds(), 10))
            .judgeFollowerDownSecondsThreshold(DefaultValueUtils
                .getOrDefault(this.schedulerConfigProperties.getJudgeFollowerDownSecondsThreshold(), 120))
            .judgeLeaderDownSecondsThreshold(DefaultValueUtils
                .getOrDefault(this.schedulerConfigProperties.getJudgeLeaderDownSecondsThreshold(), 120))
            .build();
    }

    private String getOrCreate(final String rocksPath) {
        final File dir = new File(rocksPath);
        // 如果指定的路径是不是文件夹，而是文件
        if (!dir.isDirectory()) {
            throw new IllegalStateException("RocksDB初始化失败，请指定文件夹而非文件: " + rocksPath);
        }
        // 如果指定的RocksDB的存储目录不存在,则进行创建
        if (!dir.exists()) {
            final boolean mkdirsResult = dir.mkdirs();
            if (!mkdirsResult) {
                throw new IllegalStateException("RocksDB初始化失败，创建RocksDB存储文件夹时失败: " + rocksPath);
            }
        }
        return rocksPath;
    }

    /**
     * 配置检查
     * 
     * @param consistencyParallelTaskConfigProperties 并行任务相关的配置
     * @param shardModeConfigProperties 分片模式相关配置
     * @param rocksDBConfigProperties rocksDB属性配置
     * @param peerNodeConfigProperties 集群节点配置的属性
     */
    private void doConfigCheck(final ConsistencyParallelTaskConfigProperties consistencyParallelTaskConfigProperties,
        final ShardModeConfigProperties shardModeConfigProperties,
        final RocksDBConfigProperties rocksDBConfigProperties,
        final PeerNodeConfigProperties peerNodeConfigProperties) {
        TimeUnit timeUnit = null;
        if (!StringUtils.isEmpty(consistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTimeUnit())) {
            try {
                timeUnit = TimeUnit.valueOf(consistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTimeUnit());
            } catch (final IllegalArgumentException e) {
                log.error("检查threadPoolKeepAliveTimeUnit配置时，发生异常", e);
                final String errMsg =
                    "threadPoolKeepAliveTimeUnit配置错误！注意：请在[SECONDS,MINUTES,HOURS,DAYS,NANOSECONDS,MICROSECONDS,MILLISECONDS]任选其中之一";
                throw new ConsistencyException(errMsg);
            }
        }

        if (!StringUtils.isEmpty(consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName())) {
            // 校验是否存在该类
            final Class<?> taskScheduleTimeRangeClass = ReflectTools
                .checkClassByName(consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName());
            if (ObjectUtils.isEmpty(taskScheduleTimeRangeClass)) {
                final String errMsg = String.format("未找到 %s 类，请检查类路径是否正确",
                    consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName());
                throw new ConsistencyException(errMsg);
            }
            // 用户自定义校验：校验是否实现了TaskTimeRangeQuery接口
            final boolean result =
                ReflectTools.isRealizeTargetInterface(taskScheduleTimeRangeClass, TaskTimeRangeQuery.class.getName());
            if (!result) {
                final String errMsg = String.format("%s 类，未实现TaskTimeRangeQuery接口",
                    consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName());
                throw new ConsistencyException(errMsg);
            }
        }

        if (!StringUtils.isEmpty(shardModeConfigProperties.getShardingKeyGeneratorClassName())) {
            // 校验是否存在该类
            final Class<?> shardingKeyGeneratorClass =
                ReflectTools.checkClassByName(shardModeConfigProperties.getShardingKeyGeneratorClassName());
            if (ObjectUtils.isEmpty(shardingKeyGeneratorClass)) {
                final String errMsg =
                    String.format("未找到 %s 类，请检查类路径是否正确", shardModeConfigProperties.getShardingKeyGeneratorClassName());
                throw new ConsistencyException(errMsg);
            }
            // 用户自定义校验：校验是否实现了ShardingKeyGenerator接口
            final boolean result =
                ReflectTools.isRealizeTargetInterface(shardingKeyGeneratorClass, ShardingKeyGenerator.class.getName());
            if (!result) {
                final String errMsg = String.format("%s 类，未实现ShardingKeyGenerator接口",
                    shardModeConfigProperties.getShardingKeyGeneratorClassName());
                throw new ConsistencyException(errMsg);
            }
        }

        if (StringUtils.isEmpty(rocksDBConfigProperties.rocksPath)) {
            throw new ConsistencyException("请指定RocksDB文件存储的路径，配置文件中的配置项为：consistency.rocksdb.rocks-path");
        }

        final File dir = new File(rocksDBConfigProperties.rocksPath);
        // 如果指定的路径是不是文件夹，而是文件
        if (!dir.isDirectory()) {
            throw new ConsistencyException("检查RocksDB存储路径时，发现您指定的不是文件夹而是文件: " + rocksDBConfigProperties.rocksPath);
        }
        // 如果指定的RocksDB的存储目录不存在,则进行创建
        if (!dir.exists()) {
            final boolean mkdirsResult = dir.mkdirs();
            if (!mkdirsResult) {
                throw new ConsistencyException(
                    "初始化RocksDB文件存储路径时，发生异常，请检查您配置的路径是否有访问权限或其他问题: " + rocksDBConfigProperties.rocksPath);
            }
        }

        if (StringUtils.isEmpty(peerNodeConfigProperties.getPeersConfig())) {
            throw new ConsistencyException(
                "未配置集群节点的配置信息。格式: ip1:port:id1,ip2:port:id2,ip3:port:id3，配置项为：" + "consistency.peers.peers-config");
        }

        // 如果集群节点配置了，但是集群节点id中有重复项
        if (this.peersConfigIsDuplicate()) {
            throw new ConsistencyException(
                "检查集群节点的配置信息时，发现有重复项，请仔细核对，不允许有重复的节点信息，您配置的" + "内容为" + peerNodeConfigProperties.getPeersConfig());
        }

        if (this.peerIpAndPortIsDuplicate()) {
            throw new ConsistencyException("检查集群节点的配置信息时，发现有重复的[ip:port]，请仔细核对，不允许有重复的节点信息，您配置的" + "内容为"
                + peerNodeConfigProperties.getPeersConfig());
        }

        // 检查是否有
        if (this.peerIdIsDuplicate()) {
            throw new ConsistencyException(
                "检查集群节点的配置信息时，发现[节点id]中重复项，请仔细核对，不允许有重复的节点信息，您配置的" + "内容为" + peerNodeConfigProperties.getPeersConfig());
        }

    }

    /**
     * 获取执行引擎分片数
     * 
     * @return 任务执行时，要分片的数量
     */
    private Long getTaskShardingCountOrDefault() {
        if (!ObjectUtil.isEmpty(this.shardModeConfigProperties.getTaskShardingCount())) {
            return this.shardModeConfigProperties.getTaskShardingCount();
        } else if (!StringUtils.isEmpty(this.peerNodeConfigProperties.getPeersConfig())) {
            return (long)this.peerNodeConfigProperties.getPeersConfig().split(",").length;
        } else {
            // 如果任务分片数没有设置 且 一致性框架的集群节点也没有设置 那么就将分片数设置为1
            return 1L;
        }
    }

    /**
     * 检查集群节点的配置是否有重复
     * 
     * @return 是否有重复
     */
    private boolean peersConfigIsDuplicate() {
        // 解析为list格式
        final List<String> peersConfigList =
            this.parsePeersConfigToList(this.peerNodeConfigProperties.getPeersConfig());
        // 去重
        final List<String> distinctPeersConfigList = peersConfigList.stream().distinct().collect(Collectors.toList());

        return peersConfigList.size() != distinctPeersConfigList.size();
    }

    /**
     * 解析为list格式
     * 
     * @param peersConfig 解析为list格式
     * @return 解析为list格式
     */
    private List<String> parsePeersConfigToList(final String peersConfig) {
        final String[] splitPeers = peersConfig.split(",");
        final List<String> peersConfigList = new ArrayList<>(splitPeers.length);
        peersConfigList.addAll(Arrays.asList(splitPeers));
        return peersConfigList;
    }

    /**
     * 检查集群节点的配置中是否有ip:port的重复项
     * 
     * @return 是否有ip:port的重复项
     */
    private boolean peerIpAndPortIsDuplicate() {
        final List<String> peersConfigList =
            this.parsePeersConfigToList(this.peerNodeConfigProperties.getPeersConfig());

        final List<String> peerIpAndPortList = new ArrayList<>(peersConfigList.size());
        for (final String peer : peersConfigList) {
            final String peerIpAndPort = peer.substring(0, peer.lastIndexOf(":"));
            peerIpAndPortList.add(peerIpAndPort);
        }

        // 去重
        final List<String> distinctPeerIpAndPortList =
            peerIpAndPortList.stream().distinct().collect(Collectors.toList());

        return peerIpAndPortList.size() != distinctPeerIpAndPortList.size();
    }

    /**
     * 检查集群节点的id是否有重复
     * 
     * @return 集群节点id是否有重复
     */
    private boolean peerIdIsDuplicate() {
        // 解析为list格式
        final List<String> peersConfigList =
            this.parsePeersConfigToList(this.peerNodeConfigProperties.getPeersConfig());

        final List<Integer> peerIdList = new ArrayList<>(peersConfigList.size());
        for (final String peer : peersConfigList) {
            final String peerId = peer.substring(peer.lastIndexOf(":") + 1);
            if (!NumberUtil.isNumber(peerId)) {
                throw new ConsistencyException("检查集群节点的配置信息时，发现[节点id]中存在非数字项，请仔细核对，不允许有重复的节点信息，您配置的" + "内容为"
                    + this.peerNodeConfigProperties.getPeersConfig());
            }
            peerIdList.add(Integer.parseInt(peerId));
        }

        // 去重
        final List<Integer> distinctPeerIdList = peerIdList.stream().distinct().collect(Collectors.toList());

        return peerIdList.size() != distinctPeerIdList.size();
    }
}
