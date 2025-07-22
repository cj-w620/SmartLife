package com.hmdp.config;

import org.redisson.Redisson;

import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *  Redisson配置类
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient(){
        //配置
        Config config = new Config();
        //单节点，设置地址，有密码设置密码
        config.useSingleServer().setAddress("redis://localhost:6379");
        //创建RedissonClient对象
        return Redisson.create(config);
    }
}
