package info.yangguo.yfs.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 从左向右
 * 2位: group数字后缀
 * 1位: store数字后缀
 * 2位: sequence序列号
 * 剩余位: 时间戳
 */
public class IdMaker {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static IdMaker idMaker = null;

    private long timestamp = -1L;
    private String groupId;
    private String storeId;
    private long sequence = 0L;

    private IdMaker() {
    }


    public IdMaker(String group, String store) {
        Matcher groupMatcher = Pattern.compile("group(\\d{2})$").matcher(group);
        Matcher storeMatcher = Pattern.compile("store(\\d{1})$").matcher(store);
        if (!groupMatcher.matches())
            throw new RuntimeException("group must match with group\\\\d{2}");
        else
            this.groupId = groupMatcher.group(1);
        if (!storeMatcher.matches())
            throw new RuntimeException("store must match with store\\\\d{1}");
        else
            this.storeId = storeMatcher.group(1);
    }


    public static IdMaker getInstance(String group, String store) {
        if (idMaker == null) {
            synchronized (IdMaker.class) {
                if (idMaker == null) {
                    idMaker = new IdMaker(group, store);
                }
            }
        }
        return idMaker;

    }

    public synchronized String next() {
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
            logger.error("Clock is moving backwards. Rejecting requests until {}.", this.timestamp);
            throw new RuntimeException(String.format("Clock moved backwards. Refusing to create for %d milliseconds", this.timestamp - now));
        }
        String sequenceStr;
        if (sequence < 10)
            sequenceStr = "0" + String.valueOf(sequence);
        else
            sequenceStr = String.valueOf(sequence);
        return groupId + storeId + sequenceStr + timestamp;
    }

    private long tilNextMillis(long timestamp) {
        long now = System.currentTimeMillis();
        while (now <= timestamp) {
            now = System.currentTimeMillis();
        }
        return now;
    }
}