package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *  基于Redis实现分布式锁（初级版本）
 */
public class SimpleRedisLock implements ILock{
    
    private StringRedisTemplate stringRedisTemplate;
    private String name;    //锁名称
        
    private final static String KEY_PREFIX = "lock:";
    private final static String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    /*  加载lua脚本
        静态常量，静态代码块，类初始化即加载
        泛型为返回值类型
    */
    private final static DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);    //设置返回值类型
    }

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //获取锁（使用redis的setnx特性）
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        /*
        * 因为success是Boolean类型，直接返回success会做自动拆箱，在自动拆箱过程中，如果success为null，就会报空指针。
        * 因此，我们对其做判断，这样success为null，也会返回false，避免了空指针可能性。
        * */
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        //调用lua脚本
        /*RedisScript，为了不硬编码，写入文件中。初始化类时就加载好文件，而不是每次释放锁再来加载，节省资源*/
        stringRedisTemplate.execute(UNLOCK_SCRIPT, 
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }
//    @Override
//    public void unlock() {
//        //获取线程标识
//        String threadId = ID_PREFIX + Thread.currentThread().getId();
//        //获取锁中标识
//        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        //判断锁标识是否一致，一致才能删
//        if(threadId.equals(id)){
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }
}
