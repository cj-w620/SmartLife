package com.hmdp.utils;

import lombok.Data;
import java.time.LocalDateTime;

/*
 * 为逻辑过期而生的类
 * 逻辑过期的数据并不会真的有过期时间，他是否过期是通过添加时间字段判断的，为了不改动原来的实体类，于是
 * 诞生了RedisData类，Object放实体类，LocalDateTime放过期时间
 */
@Data
public class RedisData {
    private LocalDateTime expireTime;
    private Object data;
}
