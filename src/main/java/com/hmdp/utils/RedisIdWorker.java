package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 *  Redis实现全局唯一ID生成器
 */
@Component
public class RedisIdWorker {

    /**
     * 基时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1640995200L;

    /**
     * 序列号位数
     */
    private static final int COUNT_LEN = 32;
    
    private static StringRedisTemplate stringRedisTemplate;
    
    public RedisIdWorker(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }
    
    /**
     * 生成唯一ID
     * @param keyPrefix 业务前缀
     * @return
     */
    public long nextId(String keyPrefix){
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        
        // 2.生成序列号
        // 2.1获取当前时间（精确到天）
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.2自增长
        // 因为要做运算，所以用long而不是包装类Long，报黄提示产生空指针，实际是不会产生空指针的。
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        
        // 3. 拼接返回
        // 时间戳左移序列号位数，给序列号腾位置，再或上序列号，就将序列号放上去了。
        return timestamp << COUNT_LEN | count;
    }

    public static void main(String[] args) {
        //获取基时间戳
        LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println(second);
    }
}
