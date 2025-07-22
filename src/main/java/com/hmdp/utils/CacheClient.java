package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.constant.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 *  Redis缓存工具类
 */
@Slf4j
@Component
public class CacheClient {
    
    private final StringRedisTemplate stringRedisTemplate;
    
    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 写入redis（TTL过期时间）
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    /**
     * 写入redis（逻辑过期）
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(value));
    }

    //缓存穿透
    /*
    * 不知道返回值是什么类型 ==> 定义泛型      id类型也不一定 ==> 定义泛型
    * 定义的泛型让用户传入，要求Class参数
    * 根据id查询，不知道它要查的是什么，也不知道查的是哪个数据库，于是让它传函数给我们
    * Function<ID,R>：ID为参数类型，R为返回值类型
    * 不知道它要缓存多长时间，让它传！
    * */
    /**
     * 根据id查询数据，无缓存查询数据库（以缓存空数据方式，解决缓存击穿）
     * @param keyPrefix 键前缀
     * @param id id   
     * @param type  查询数据类型
     * @param dbFallback    查询数据库语句（如果无缓存数据，需要查询数据库）
     * @param time  缓存时间
     * @param unit  缓存时间单位
     * @return  返回查询数据
     * @param <R>
     * @param <ID>
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = keyPrefix + id;
        //去redis查询缓存中是否存在数据
        String json = stringRedisTemplate.opsForValue().get(key);

        //存在，直接返回
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        
        //判断缓存中的值是否为缓存的空值（解决缓存穿透问题）
        //没有通过isNotBlank判断，证明要么为空字符串，要么为null（不存在）
        //如果为空字符串就是我们缓存的空数据，证明数据库没有，直接返回，解决缓存穿透。如果为null，就证明没有缓存，去数据库查询。
        if(json != null){   //不是null就是空字符串，直接返回空
            return null;
        }
        
        //根据id去数据库查询
        R r = dbFallback.apply(id);
        //数据库不存在该数据
        if(r == null){
            //缓存空值到redis中（为解决缓存穿透问题）
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        //数据库存在该数据
        //缓存到redis中
        this.set(key,r,time,unit);
        //5. 返回
        return r;
    }
    
    
    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //逻辑过期解决缓存击穿
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = keyPrefix + id;
        //去redis查询缓存中是否存在数据
        String json = stringRedisTemplate.opsForValue().get(key);

        //不存在，返回空
        if(StrUtil.isBlank(json)){
            return null;
        }

        //存在，将缓存数据Json字符串反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject, type);
        //判断逻辑过期
        if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
            //未过期，直接返回数据
            return r;
        }

        //过期
        //获取互斥锁
        String localKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(localKey);
        //成功，开新线程重建缓存，返回过期数据
        if(isLock){
            //Double Check
            //获取锁成功，再次检测redis缓存是否存在，如果存在，则无需重建缓存
            json = stringRedisTemplate.opsForValue().get(key);
            redisData = JSONUtil.toBean(json, RedisData.class);
            jsonObject = (JSONObject) redisData.getData();
            r = JSONUtil.toBean(jsonObject, type);
            if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
                return r;
            }

            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //开启新线程去重建缓存
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入缓存
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放互斥锁
                    unlock(localKey);
                }
            });
        }
        //5.2 失败，直接返回数据
        //6. 返回
        return r;
    }

    //获取锁
    private boolean tryLock(String key){
        //setIfAbsent就是setnx操作。值是随便给的
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);    
    }
    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
