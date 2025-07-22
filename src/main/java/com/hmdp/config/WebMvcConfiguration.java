package com.hmdp.config;

import com.hmdp.interceptor.LoginInterceptor;
import com.hmdp.interceptor.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

/**
 *  webmvc配置类
 */
@Configuration
public class WebMvcConfiguration implements WebMvcConfigurer {
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LoginInterceptor())
                .excludePathPatterns(
                        "/user/code",
                        "/user/login",
                        "/blog/hot",
                        "/shop-type/**",
                        "/shop/**",
                        "/upload/**",
                        "/voucher/**",
                        "/voucher-order/**"
                ).order(1);
        //默认拦截路径就是/**，这里显示写出来。若不指定order，默认order都是0，拦截器顺序就按添加顺序来。
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate)).addPathPatterns("/**").order(0);
    }
}
