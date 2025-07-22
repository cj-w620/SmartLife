package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.constant.RedisConstants;
import com.hmdp.constant.SystemConstants;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;
    
    /**
     * 根据id查询商铺信息
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = queryWithPassThrough(id);
        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, new Function<Long, Shop>() {
            @Override
            public Shop apply(Long aLong) {
                return getById(aLong);
            }
        },RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);  //使用缓存工具类版本写法
        
        //互斥锁解决缓存击穿（并带着解决缓存穿透）
//        Shop shop = queryWithMutex(id);
        
        //逻辑过期解决缓存击穿（并带着解决缓存穿透）
//        Shop shop = queryWithLogicalExpire(id);
        if(shop == null){
            return Result.fail("商铺信息不存在");
        }
        return Result.ok(shop);
    }
    
    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //逻辑过期解决缓存击穿
    public Shop queryWithLogicalExpire(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.去redis查询缓存中是否存在商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.不存在，返回空
        if(StrUtil.isBlank(shopJson)){
            return null;
        }
        
        //3.存在，将缓存数据Json字符串反序列化为对象
        /*
        *  对象属性被反序列化后是JSONObject。redisData.getData()不能直接强转为shop，也不能在redisData的data属性加泛型。 
        *  这应该是hutool包的问题
        * */
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(jsonObject, Shop.class);
        //4.判断逻辑过期
        if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
            //4.1 未过期，直接返回数据
            return shop;
        }
        
        //4.2 过期
        //5.获取互斥锁
        String localKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(localKey);
        //5.1 成功，开新线程重建缓存，返回过期商铺数据
        if(isLock){
            //Double Check
            //获取锁成功，再次检测redis缓存是否存在，如果存在，则无需重建缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            redisData = JSONUtil.toBean(shopJson, RedisData.class);
            jsonObject = (JSONObject) redisData.getData();
            shop = JSONUtil.toBean(jsonObject, Shop.class);
            if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
                return shop;
            }
            
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //6. 开启新线程去重建缓存
                try {
                    //正常是应该设置半小时，这里为了方便测试，设置为20s
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放互斥锁
                    unlock(localKey);
                }
            });
        }
        //5.2 失败，直接返回商铺信息
        //6. 返回
        return shop;
    }
    
    //互斥锁解决缓存击穿
    public Shop queryWithMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.去redis查询缓存中是否存在商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.1 存在，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //判断缓存中的值是否为缓存的空值
        if(shopJson != null){   
            return null;
        }

        //2.2 不存在
        //3.获取互斥锁
        //每个店铺一个锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.判断获取锁是否成功
            if(!isLock){
                //4.1 失败，休眠后重新从查询缓存开始
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.2 成功
            //获取锁成功，再次检测redis缓存是否存在，如果存在，则无需重建缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isNotBlank(shopJson)){
                return JSONUtil.toBean(shopJson, Shop.class); 
            }
            //重建缓存
            //5.根据id去数据库查询商铺信息
            shop = getById(id);
            //5.1商铺信息在数据库不存在
            if(shop == null){
                //缓存空值到redis中
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //5.2存在
            //将商铺信息缓存到redis中
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //6.释放锁
            unlock(lockKey);
        }
        //7. 返回
        return shop;
    }
    
    //缓存穿透
    public Shop queryWithPassThrough(Long id){
        /* 这里用String类型存储商铺信息到redis中，用hash也可以。在redis中，以JSON格式存储商铺信息 */
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.去redis查询缓存中是否存在商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.1 存在，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //判断缓存中的值是否为缓存的空值（解决缓存穿透问题）
        //shopJson为null，就是没查到，是要去数据库查的；为空字符串，就是一个缓存的空值，即在数据库中不存在，不能走数据库，直接返回。
        if(shopJson != null){   //不是null就是空字符串，有数据的话在上面的if就返回了。
            return null;
        }

        //2.2 不存在
        //3.根据id去数据库查询商铺信息
        Shop shop = getById(id);
        //3.1 不存在，返回错误提示
        if(shop == null){
            //缓存空值到redis中（为解决缓存穿透问题）
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        //3.2 存在
        //4. 将商铺信息缓存到redis中
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //5. 返回
        return shop;
    }

    //提前缓存逻辑过期店铺信息到redis中
    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        //查询店铺信息
        Shop shop = getById(id);
        Thread.sleep(200);  //为方便看测试结果
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //存入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }
    
    /**
     * 获取锁
     * @param key
     * @return  true成功  false失败
     */
    private boolean tryLock(String key){
        //setIfAbsent就是setnx操作。值是随便给的
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);    //不能直接返回flag，因为java会对其进行拆箱，可能会产生空指针
    }

    /**
     * 释放锁
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 更新商铺信息
     * @param shop
     * @return
     */
    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("商铺id不存在");
        }
        //1.操作数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    //根据商店类型查询商铺（附近商铺）
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否是根据位置查询
        if(x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.根据位置查询 GEOSEARCH key FROMLONLAT longitude latitude BYRADIUS radius KM WITHDIST
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        //4.解析数据
        if(results == null){    //判断非空
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();  //拿到数据
        if(list.size() <= from){
            //没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());  //放shopid,后面去数据库查shop信息
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        /*
        * 因为redistemplate只提供limit(long value)，取出从0到value。所以我们要分页，只能手动切割，把前from条去掉
        * */
        list.stream().skip(from).forEach(result -> {       //跳过前from条
            //获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取距离 
            distanceMap.put(shopIdStr,result.getDistance());
        });
        
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id, "+ idStr +")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //5.返回（店铺信息、位置）
        return Result.ok(shops);
    }
}
