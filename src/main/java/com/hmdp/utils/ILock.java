package com.hmdp.utils;


public interface ILock {

    /**
     * 获取锁
     * @param timeoutSec    超时时间，过期锁自动释放
     * @return  true代表获取锁成功，false代表获取锁失败
     */
    boolean tryLock(long timeoutSec);
    
    /**
     * 释放锁
     */
    void unlock();
}
