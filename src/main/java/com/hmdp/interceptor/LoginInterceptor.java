package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.constant.RedisConstants;
import com.hmdp.dto.UserDTO;

import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



/**
 *  登录拦截器
 */
/*
* 最初，是在请求内做登录校验，随着请求越来越多，如果每个请求都写登录校验，都是重复的代码，太过冗余，
* 于是登录拦截器就出现了。但在拦截器做请求拦截，控制器中如何拿到用户信息，如果每个请求都引入session去拿，
* 也太过麻烦。所以就在拦截器中，将用户信息放到线程里，供控制器使用。每个请求进来，都会有独立的ThreadLocal，
* 避免请求间相互干扰
* */
public class LoginInterceptor implements HandlerInterceptor {
    
    //前置拦截
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //判断用户信息是否存在ThreadLocal中，即判断是否登录
        if(UserHolder.getUser() == null){
           return false; 
        }
        
        //放行
        return true;
    }
    
}
