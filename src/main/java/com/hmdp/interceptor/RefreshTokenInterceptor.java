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
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  刷新token拦截器（拦截一切请求）
 */

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;
    
    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }
    
    //前置拦截
    //若不符合要求，直接放行，拦截动作交给登录拦截器处理，该拦截器只做刷新token动作。
    //当然，没有登录，就没有token，自然没有刷新token动作，所以直接放过去让登录拦截器拦截。
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.获取session
//        HttpSession session = request.getSession();
        //1. 从请求头中拿到token
        String token = request.getHeader("Authorization");
        //判断token是否存在，不在直接放行
        if(StrUtil.isBlank(token)){
            return true;
        }
        
        //2.从session中拿出用户信息
//        User user = (User)session.getAttribute(SystemConstants.SESSION_KEY_USER);
        //2. 用token去redis中拿到用户数据
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(tokenKey);
        //3. 判断用户信息是否存在
        if(userMap.isEmpty()){
            return true;
        }
        //4. 存在
        //4.1 将map转换为UserDto
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        //4.2 保存用户信息到线程中
        UserHolder.saveUser(userDTO);
        
        //5. 刷新token过期时间
        stringRedisTemplate.expire(tokenKey,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        //6. 放行
        return true;
    }

    //渲染后返回前拦截
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
