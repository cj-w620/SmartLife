package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    //lua脚本
    private final static DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);    //设置返回值类型
    }

    //阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    //线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    
    /*
        一旦项目启动，用户就有可能抢购订单，所以，应在一开始就提交任务，处理阻塞队列。
        通过@PostConstruct，在类一开始加载时，就提交任务。*/
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    
    //任务
    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断获取订单是否成功
                    if(list == null || list.size() == 0){
                        // 2.1 失败：continue重试
                        continue;
                    }
                    // 2.2 成功：
                    // 3.解析出订单信息 
                    /*我们明确知道每次只取一条，直接拿就好*/
                    /*string是消息id，两个Object，是因为我们存入消息时，用的是key-value形式，于是它这里就是key和value*/
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.实际创建订单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认 sack stream.orders g1 id //id是消息id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("创建订单异常",e);
                    /*处理订单有异常，去pending-list中处理异常订单*/
                    handlePendingListOrders();
                }
                
            }
        }

        private void handlePendingListOrders() {
            while (true){
                try {
                    // 1.获取pending-list队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    /*pending-list没有block*/
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2.判断获取订单是否成功
                    if(list == null || list.size() == 0){
                        //pending-list中没有异常订单，直接跳出
                        break;
                    }
                    // 2.2 成功：
                    // 3.解析出订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.实际创建订单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认 sack stream.orders g1 id //id是消息id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("pending-list订单处理异常",e);
                    /*如果在处理pending-list订单过程中又出现异常，捕捉到后，又会接着走while，再次处理*/
                    /*为了不要太过频繁，让它休眠一下*/
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        /*这里的userId不能再从UserHolder中去拿了，因为这是异步开启的，线程池中的线程，不再是之前的主线程了*/
        //获取用户id
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        //判断获取锁是否成功
        if(!isLock){
            log.error("不可重复下单");
            return;
        }
        try {
            //创建订单
            proxy.createOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    /**
     * 抢购优惠券（秒杀优化版本二）（redis stream消息队列版）
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //生成订单id
        long orderId = redisIdWorker.nextId("order");
        //1. 调用lua脚本  /*没有key，传空集合，不能传null*/
        /*lua脚本中参数都是String类型，传参时，注意要将参数类型转为String*/
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString(), String.valueOf(orderId));
        //2. 判断返回结果
        int r = result.intValue();
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足" : "请不要重复下单");
        }
        
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //4. 将订单id返回
        return Result.ok(orderId);
    }
    
//    /**
//     * 抢购优惠券（秒杀优化版本一）（阻塞队列版）
//     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户id
//        Long userId = UserHolder.getUser().getId();
//        //1. 调用lua脚本  /*没有key，传空集合，不能传null*/
//        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId, userId);
//        //2. 判断返回结果
//        int r = result.intValue();
//        if(r != 0){
//            return Result.fail(r == 1 ? "库存不足" : "请不要重复下单");
//        }
//        //3.将信息放入阻塞队列，异步开启线程执行数据库操作
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //优惠券id
//        voucherOrder.setVoucherId(voucherId);
//        //用户id
//        voucherOrder.setUserId(userId);
//        //放入阻塞队列
//        orderTasks.add(voucherOrder);
//        //获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        
//        //4. 将订单id返回
//        return Result.ok(orderId);   
//    }

    @Transactional
    public void createOrder(VoucherOrder voucherOrder) {
        //一人一单（高并发）
        //4.判断用户是否已下过单
        /*这里同样不能通过UserHolder取userId*/
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("不可重复下单！");
            return;
        }
        // 5.扣减库存
        //解决超卖（高并发）：加乐观锁：gt("stock",0)，在修改数据前，查看库存信息如果大于0，则可以卖。
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            log.error("没货噜");
            return;
        }
        // 6.插入订单
        save(voucherOrder);
    }
    
//    /**
//     * 抢购优惠券（秒杀优化前版本）
//     */
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断是否在活动时间内
//        // 2.1 活动开始前
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("活动尚未开始！想干嘛（指）");
//        }
//        // 2.2 活动结束后
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("活动已经结束😔，下次早点来啦");
//        }
//        // 3.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("没货噜");
//        }
//        Long userId = UserHolder.getUser().getId();
//        
//        //获取自定义锁
//        /*锁名称拼上用户id，就是将锁的范围缩小到每个用户，同一个用户的重复下单会被锁*/
////        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate,"order:" + userId);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        /*
//        * lock.tryLock(锁获取失败最大等待时间，超时释放时间，时间单位);
//        * 当前一人一单业务，不需要重试，直接返回失败，所以不传参。
//        * */
//        boolean isLock = lock.tryLock();
//        //判断获取锁是否成功
//        /*失败了有两种处理：①重试；②返回失败*/
//        /*一人一单业务，如果获取锁失败，证明同一个用户同时下了多单，不可能让他重试，直接返回失败*/
//        if(!isLock){
//            return Result.fail("不可重复下单");
//        }
//        try {
//            //获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createOrder(voucherId);
//        } finally {
//            //释放锁
//            lock.unlock();
//        }
//
//    }

//    @Transactional
//    public Result createOrder(Long voucherId) {
//        //一人一单（高并发）
//        //4.判断用户是否已下过单
//        Long userId = UserHolder.getUser().getId();
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        if (count > 0) {
//            return Result.fail("不可重复下单！");
//        }
//        // 5.扣减库存
//        //解决超卖（高并发）：加乐观锁：gt("stock",0)，在修改数据前，查看库存信息如果大于0，则可以卖。
//        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId).gt("stock", 0)
//                .update();
//        if (!success) {
//            return Result.fail("没货噜");
//        }
//        // 6.插入订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 6.1 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 6.2 优惠券id
//        voucherOrder.setVoucherId(voucherId);
//        // 6.3 用户id
//        voucherOrder.setUserId(userId);
//        save(voucherOrder);
//        // 7.返回订单id
//        return Result.ok(orderId);
//    }
    
}
