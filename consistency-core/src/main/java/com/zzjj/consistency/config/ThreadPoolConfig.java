package com.zzjj.consistency.config;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.zzjj.consistency.controller.data.RegisterOrCancelResponse;
import com.zzjj.consistency.model.ConsistencyTaskInstance;

/**
 * 线程池配置
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Configuration
public class ThreadPoolConfig {

    /**
     * 一致性任务线程名称前缀
     */
    private static final String CONSISTENCY_TASK_THREAD_POOL_PREFIX = "CTThreadPool_";
    /**
     * 用于节点新增和取消的线程池名称前缀
     */
    private static final String PEER_REGISTER_THREAD_POOL_PREFIX = "PeerAddOrCancelPool_";

    /**
     * 告警线程名称的前缀
     */
    private static final String ALERT_THREAD_POOL_PREFIX = "AlertThreadPool_";

    /**
     * 获取框架级的配置
     */
    @Autowired
    private ConsistencyConfiguration consistencyConfiguration;

    /**
     * 一致性任务执行的并行任务执行线程池
     *
     * @return 并行任务线程池
     */
    @Bean
    public CompletionService<ConsistencyTaskInstance> consistencyTaskPool() {
        final LinkedBlockingQueue<Runnable> asyncConsistencyTaskThreadPoolQueue =
            new LinkedBlockingQueue<>(this.consistencyConfiguration.getThreadPoolQueueSize());
        final ThreadPoolExecutor asyncReleaseResourceExecutorPool =
            new ThreadPoolExecutor(this.consistencyConfiguration.getThreadCorePoolSize(),
                this.consistencyConfiguration.getThreadCorePoolSize(),
                this.consistencyConfiguration.getThreadPoolKeepAliveTime(),
                TimeUnit.valueOf(this.consistencyConfiguration.getThreadPoolKeepAliveTimeUnit()),
                asyncConsistencyTaskThreadPoolQueue,
                this.createThreadFactory(ThreadPoolConfig.CONSISTENCY_TASK_THREAD_POOL_PREFIX));
        return new ExecutorCompletionService<>(asyncReleaseResourceExecutorPool);
    }

    @Bean
    public CompletionService<RegisterOrCancelResponse> peerRegisterPool() {
        final LinkedBlockingQueue<Runnable> asyncConsistencyTaskThreadPoolQueue = new LinkedBlockingQueue<>(50);
        final ThreadPoolExecutor asyncReleaseResourceExecutorPool =
            new ThreadPoolExecutor(5, 5, 60, TimeUnit.SECONDS, asyncConsistencyTaskThreadPoolQueue,
                this.createThreadFactory(ThreadPoolConfig.PEER_REGISTER_THREAD_POOL_PREFIX));
        return new ExecutorCompletionService<>(asyncReleaseResourceExecutorPool);
    }

    /**
     * 用于告警通知的线程池
     *
     * @return 并行任务线程池
     */
    @Bean
    public ThreadPoolExecutor alertNoticePool() {
        final LinkedBlockingQueue<Runnable> asyncAlertNoticeThreadPoolQueue = new LinkedBlockingQueue<>(100);
        return new ThreadPoolExecutor(3, 5, 60, TimeUnit.SECONDS, asyncAlertNoticeThreadPoolQueue,
            this.createThreadFactory(ThreadPoolConfig.ALERT_THREAD_POOL_PREFIX));
    }

    /**
     * 创建线程池工厂
     *
     * @param threadPoolPrefix 线程池前缀
     * @return 线程池工厂
     */
    private ThreadFactory createThreadFactory(final String threadPoolPrefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, threadPoolPrefix + this.threadIndex.incrementAndGet());
            }
        };
    }

}
