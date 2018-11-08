package info.yangguo.yfs.common.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 从左向右
 * 2位: group数字后缀
 * 1位: store数字后缀
 * 2位: sequence序列号
 * 剩余位: 时间戳
 */
public enum IdMaker {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(IdMaker.class);
    private long timestamp = -1L;
    private long sequence = 0L;

    public synchronized String next(String group, String store) {
        String groupId;
        String storeId;
        Matcher groupMatcher = Pattern.compile("group(\\d{2})$").matcher(group);
        Matcher storeMatcher = Pattern.compile("store(\\d{1})$").matcher(store);
        if (!groupMatcher.matches())
            throw new RuntimeException("group must match with group\\\\d{2}");
        else
            groupId = groupMatcher.group(1);
        if (!storeMatcher.matches())
            throw new RuntimeException("store must match with store\\\\d{1}");
        else
            storeId = storeMatcher.group(1);
        long now = System.currentTimeMillis();
        if (now > this.timestamp) {
            this.timestamp = now;
            this.sequence = 0L;
        } else if (now == this.timestamp) {
            this.sequence++;
            if (this.sequence > 99) {
                this.timestamp = tilNextMillis(this.timestamp);
                this.sequence = 0L;
            }
        } else if (now < this.timestamp) {
            LOGGER.error("Clock is moving backwards. Rejecting requests until {}.", this.timestamp);
            throw new RuntimeException(String.format("Clock moved backwards. Refusing to create for %d milliseconds", this.timestamp - now));
        }
        String sequenceStr;
        if (sequence < 10)
            sequenceStr = "0" + String.valueOf(sequence);
        else
            sequenceStr = String.valueOf(sequence);
        return groupId + storeId + sequenceStr + timestamp;
    }

    public String[] split(String id) {
        String[] parts = new String[4];
        parts[0] = id.substring(0, 2);
        parts[1] = id.substring(2, 3);
        parts[2] = id.substring(3, 5);
        parts[3] = id.substring(5);
        return parts;
    }

    private long tilNextMillis(long timestamp) {
        long now = System.currentTimeMillis();
        while (now <= timestamp) {
            now = System.currentTimeMillis();
        }
        return now;
    }
}