package com.zzjj.consistency.custom.shard;

import java.util.Calendar;
import java.util.Properties;

import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

/**
 * 雪花分片Id生产器
 * 
 * @author zengjin
 * @date 2023/11/19
 **/
public class SnowflakeShardingKeyGenerator implements ShardingKeyGenerator {

    private static volatile SnowflakeShardingKeyGenerator instance;

    public static final long EPOCH;

    private static final long SEQUENCE_BITS = 12L;

    private static final long WORKER_ID_BITS = 10L;

    private static final long SEQUENCE_MASK = (1 << SnowflakeShardingKeyGenerator.SEQUENCE_BITS) - 1;

    private static final long WORKER_ID_LEFT_SHIFT_BITS = SnowflakeShardingKeyGenerator.SEQUENCE_BITS;

    private static final long TIMESTAMP_LEFT_SHIFT_BITS =
        SnowflakeShardingKeyGenerator.WORKER_ID_LEFT_SHIFT_BITS + SnowflakeShardingKeyGenerator.WORKER_ID_BITS;

    private static final long WORKER_ID_MAX_VALUE = 1L << SnowflakeShardingKeyGenerator.WORKER_ID_BITS;

    private static final long WORKER_ID = 0;

    private static final int MAX_TOLERATE_TIME_DIFFERENCE_MILLISECONDS = 10;

    private SnowflakeShardingKeyGenerator() {}

    @Getter
    @Setter
    private Properties properties = new Properties();

    private byte sequenceOffset;

    private long sequence;

    private long lastMilliseconds;

    static {
        final Calendar calendar = Calendar.getInstance();
        calendar.set(2016, Calendar.NOVEMBER, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        EPOCH = calendar.getTimeInMillis();
    }

    /**
     * 生产一致性任务分片键
     *
     * @return 一致性任务分片键
     */
    @Override
    public synchronized long generateShardKey() {
        long currentMilliseconds = System.currentTimeMillis();
        if (this.waitTolerateTimeDifferenceIfNeed(currentMilliseconds)) {
            currentMilliseconds = System.currentTimeMillis();
        }
        if (this.lastMilliseconds == currentMilliseconds) {
            if (0L == (this.sequence = (this.sequence + 1) & SnowflakeShardingKeyGenerator.SEQUENCE_MASK)) {
                currentMilliseconds = SnowflakeShardingKeyGenerator.waitUntilNextTime(currentMilliseconds);
            }
        } else {
            this.vibrateSequenceOffset();
            this.sequence = this.sequenceOffset;
        }
        this.lastMilliseconds = currentMilliseconds;
        // 生成雪花算法id
        return ((currentMilliseconds
            - SnowflakeShardingKeyGenerator.EPOCH) << SnowflakeShardingKeyGenerator.TIMESTAMP_LEFT_SHIFT_BITS)
            | (this.getWorkerId() << SnowflakeShardingKeyGenerator.WORKER_ID_LEFT_SHIFT_BITS) | this.sequence;
    }

    /**
     * 判断当前时间是否出现时间回拨
     * 
     * @param currentMilliseconds 当前时间
     * @return 是否需要重新获取时间
     */
    @SneakyThrows
    private boolean waitTolerateTimeDifferenceIfNeed(final long currentMilliseconds) {
        if (this.lastMilliseconds <= currentMilliseconds) {
            return false;
        }
        final long timeDifferenceMilliseconds = this.lastMilliseconds - currentMilliseconds;
        Preconditions.checkState(timeDifferenceMilliseconds < this.getMaxTolerateTimeDifferenceMilliseconds(),
            "Clock is moving backwards, last time is %d milliseconds, current time is %d milliseconds",
            this.lastMilliseconds, currentMilliseconds);
        Thread.sleep(timeDifferenceMilliseconds);
        return true;
    }

    private long getWorkerId() {
        final long result = Long.parseLong(
            this.properties.getProperty("worker.id", String.valueOf(SnowflakeShardingKeyGenerator.WORKER_ID)));
        Preconditions.checkArgument(result >= 0L && result < SnowflakeShardingKeyGenerator.WORKER_ID_MAX_VALUE);
        return result;
    }

    private int getMaxTolerateTimeDifferenceMilliseconds() {
        return Integer.parseInt(this.properties.getProperty("max.tolerate.time.difference.milliseconds",
            String.valueOf(SnowflakeShardingKeyGenerator.MAX_TOLERATE_TIME_DIFFERENCE_MILLISECONDS)));
    }

    /**
     * 等待下一时间
     * 
     * @param lastTime 最后的时间
     * @return 返回雪花算法的时间
     */
    private static long waitUntilNextTime(final long lastTime) {
        long result = System.currentTimeMillis();
        while (result <= lastTime) {
            result = System.currentTimeMillis();
        }
        return result;
    }

    private void vibrateSequenceOffset() {
        this.sequenceOffset = (byte)(~this.sequenceOffset & 1);
    }

    /**
     * 获取单例对象
     *
     * @return 单例对象
     */
    public static SnowflakeShardingKeyGenerator getInstance() {
        if (SnowflakeShardingKeyGenerator.instance == null) {
            synchronized (SnowflakeShardingKeyGenerator.class) {
                if (SnowflakeShardingKeyGenerator.instance == null) {
                    SnowflakeShardingKeyGenerator.instance = new SnowflakeShardingKeyGenerator();
                }
            }
        }
        return SnowflakeShardingKeyGenerator.instance;
    }

    public void setWorkerId(final String value) {
        this.properties.put("worker.id", value);
    }

}
