package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    //线程池
    private ExecutorService es = Executors.newFixedThreadPool(500);
    //测试高并发情况下ID生成情况
//    @Test
//    void testIDWordker() throws InterruptedException {
//        //因为多线程异步执行，要用latch，300个线程，让latch值达到300次。
//        CountDownLatch latch = new CountDownLatch(300);
//        //创建任务
//        Runnable task = () -> {
//            for (int i = 0; i < 100; i++) { //每次任务生成100个id
//                long id = redisIdWorker.nextId("order");
//                System.out.println("id = " + id);
//            }  
//            latch.countDown();  //每次任务完成latch+1
//        };
//        long begin = System.currentTimeMillis();
//        //提交任务
//        for (int i = 0; i < 300; i++) { //提交300次任务
//            es.submit(task);
//        }
//        latch.await();  //等到latch达到300次
//        long end = System.currentTimeMillis();
//        //1个任务100id，300次任务，总共30000个id
//        System.out.println("time = " + (end - begin));
//    }
//    
//    @Test
//    public void testSaveShop2Redis(){
////        shopService.saveShop2Redis(1L,10L);
//    }
//    
//    @Test
//    public void testRedisIncrement(){
//        for(int i = 0;i < 10;i++){
//            System.out.println(stringRedisTemplate.opsForValue().increment("hhh"));
//        }
//    }
//    
//    //导入数据库店铺数据到redis中，为geo查询附近店铺
//    @Test
//    public void addGeoData(){
//        //1.查询店铺信息（数据量多，可以分批查询，我们这不多，直接全部查）
//        List<Shop> list = shopService.list();
//        //2.把店铺分组，按照typeId分组，typeId一致是一个集合
//        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
//        //3.分批完成写入Redis
//        for(Map.Entry<Long,List<Shop>> entry : map.entrySet()){
//            Long typeId = entry.getKey();
//            String key = "shop:geo:"+typeId;
//            List<Shop> value = entry.getValue();
//            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
//            for(Shop shop : value){
//                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),new Point(shop.getX(),shop.getY())));
//            }
//            stringRedisTemplate.opsForGeo().add(key,locations);
//        }
//    }
//    
//    //测试hyperloglog存储1000000条数据的内存占用情况和统计效果
//    //存储前，redis内存占用：1999192字节
//    //存储后，redis内存占用：2065192字节
//    //统计结果：997593
//    @Test
//    public void testHyperloglog(){
//        String[] s = new String[1000];
//        int j;
//        for(int i = 0;i < 1000000;i++){
//            j = i % 1000;
//            s[j] = "user_" + i;
//            //1000条写1次
//            if(j == 999){
//                stringRedisTemplate.opsForHyperLogLog().add("hl2",s);
//            }
//        }
//        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl2");
//        System.out.println(count);
//    }
    
    @Test
    public void testOpenAiReviewJar(){
        System.out.println("test");
    }
}
