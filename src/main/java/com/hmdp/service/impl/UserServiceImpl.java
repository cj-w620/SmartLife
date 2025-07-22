package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.constant.SystemConstants;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.BitFieldArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constant.RedisConstants.*;

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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    /**
     * 发送短信验证码并保存验证码
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号码格式
        if(RegexUtils.isPhoneInvalid(phone)){
            //2.1 不符合，返回错误信息
            return Result.fail("手机号码格式错误");
        }
        
        //2.2 符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //3.验证码存入session
//        session.setAttribute(SystemConstants.SESSION_KEY_CODE,code);'
        //3.验证码存入redis，并设置过期时间
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //4.发送验证码
        log.debug("发送验证码：{}，到手机号码为：{}",code,phone);
        return Result.ok();
    }

    /**
     * 实现登录功能
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        //1. 校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号码格式错误");
        }
        
        //2. 校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if(cacheCode == null || !cacheCode.equalsIgnoreCase(loginForm.getCode())){
            return Result.fail("验证码失效或验证码错误");
        }   
        
        //3. 根据手机号查询用户
        User user = query().eq("phone", phone).one();
        
        //3.1 用户不存在，创建新用户，插入到数据库（注册）
        if(user == null){
            user = createUserByPhone(phone);
        }
        
        //4. 保存用户信息到session（屏蔽敏感信息，转为UserDto存入）
//        session.setAttribute(SystemConstants.SESSION_KEY_USER, BeanUtil.copyProperties(user, UserDTO.class));
        //4.保存用户信息到redis
        //4.1 随机生成token作为登录令牌
        String token = UUID.randomUUID().toString(true);
        
        //4.2 将UserDto对象转换为HashMap（为用hash形式存储）
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(), 
                CopyOptions.create().setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString())); //将对象转为map时，值全部设置为String类型，否则用stringRedisTemplate存数据到redis会报错。
        
        //4.3 以token为key，将用户信息存入redis
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        
        //4.4 设置token过期时间（30分钟）
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);
        
        //5.返回token
        return Result.ok(token);
    }

    @Override
    public Result userSign() {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2.获取当前日期
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        //3.获取当前是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //4.拼接key
        String key = USER_SIGN_KEY + userId + keySuffix;
        //5.写入Redis SETBIT key offset 1
        /*下标从0开始，所以offset=dayOfMonth-1，true就是1*/
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth-1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2.获取当前日期
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        //3.获取当前是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //4.拼接key
        String key = USER_SIGN_KEY + userId + keySuffix;
        //5.获取当前用户本月截至到今天的签到数据 BITFIELD sign:1:202502 GET u12 0
        /*offset始终给0，因为要本月截至到今天的数据。今天是几号，就查几条*/
        /*为什么返回值是个list，因为BITFIELD可以同时做set、get等操作，可能有多个结果。我们这里明确知道只做了get，所以取第1个就可以了*/
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key, 
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if(result == null || result.size() == 0){
            //没有签到数据
            return Result.ok(0);
        }
        Long num = result.get(0);   //获取签到数据
        if(num == null || num == 0){
            return Result.ok(0);
        }
        //从后往前获取每个bit位
        //循环，将数据与1做与运算，得到最后一个bit位
        int count = 0;
        while (true){
            if((num & 1) == 0){
                //如果为0，证明没签到，连续签到中断，结束
                break;
            }else{
                //如果不为0，证明签到了，计数器+1
                count++;
            }
            //num右移一位，将最后一位抛弃，倒数第二位变为最后一位
            num >>>= 1;
        }
        return Result.ok(count);
    }

    /**
     * 根据手机号码创建新用户
     * @param phone
     * @return
     */
    private User createUserByPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
