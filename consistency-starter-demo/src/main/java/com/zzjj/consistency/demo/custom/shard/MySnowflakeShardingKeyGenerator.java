package com.zzjj.consistency.demo.custom.shard;

import java.util.Calendar;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.zzjj.consistency.custom.shard.ShardingKeyGenerator;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

/**
 * @author zengjin
 * @date 2023/12/10 12:31
 **/
public class MySnowflakeShardingKeyGenerator implements ShardingKeyGenerator {

    public static final long EPOCH;

    private static final long SEQUENCE_BITS = 12L;

    private static final long WORKER_ID_BITS = 10L;

    private static final long SEQUENCE_MASK = (1 << MySnowflakeShardingKeyGenerator.SEQUENCE_BITS) - 1;

    private static final long WORKER_ID_LEFT_SHIFT_BITS = MySnowflakeShardingKeyGenerator.SEQUENCE_BITS;

    private static final long TIMESTAMP_LEFT_SHIFT_BITS =
        MySnowflakeShardingKeyGenerator.WORKER_ID_LEFT_SHIFT_BITS + MySnowflakeShardingKeyGenerator.WORKER_ID_BITS;

    private static final long WORKER_ID_MAX_VALUE = 1L << MySnowflakeShardingKeyGenerator.WORKER_ID_BITS;

    private static final long WORKER_ID = 0;

    private static final int MAX_TOLERATE_TIME_DIFFERENCE_MILLISECONDS = 10;

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
            if (0L == (this.sequence = (this.sequence + 1) & MySnowflakeShardingKeyGenerator.SEQUENCE_MASK)) {
                currentMilliseconds = MySnowflakeShardingKeyGenerator.waitUntilNextTime(currentMilliseconds);
            }
        } else {
            this.vibrateSequenceOffset();
            this.sequence = this.sequenceOffset;
        }
        this.lastMilliseconds = currentMilliseconds;
        return ((currentMilliseconds
            - MySnowflakeShardingKeyGenerator.EPOCH) << MySnowflakeShardingKeyGenerator.TIMESTAMP_LEFT_SHIFT_BITS)
            | (this.getWorkerId() << MySnowflakeShardingKeyGenerator.WORKER_ID_LEFT_SHIFT_BITS) | this.sequence;
    }

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
        final long result = Long.valueOf(
            this.properties.getProperty("worker.id", String.valueOf(MySnowflakeShardingKeyGenerator.WORKER_ID)));
        Preconditions.checkArgument(result >= 0L && result < MySnowflakeShardingKeyGenerator.WORKER_ID_MAX_VALUE);
        return result;
    }

    private int getMaxTolerateTimeDifferenceMilliseconds() {
        return Integer.valueOf(this.properties.getProperty("max.tolerate.time.difference.milliseconds",
            String.valueOf(MySnowflakeShardingKeyGenerator.MAX_TOLERATE_TIME_DIFFERENCE_MILLISECONDS)));
    }

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
}
