package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;
    
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //1. 获取当前用户
        Long currentUserId = UserHolder.getUser().getId();
        String key = "follows:" + currentUserId;
        //2. 判断是关注还是取关
        if(isFollow){
            //3. 关注：增加一条记录
            Follow follow = new Follow();
            follow.setUserId(currentUserId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if(isSuccess){
                //将关注用户的id存入set集合（为共同关注服务）
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        }else{
            //4. 取关：删除一条记录
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", currentUserId).eq("follow_user_id", followUserId));
            //将关注用户的id从set集合中移除（为共同关注服务）
            if(isSuccess){
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        //1. 获取当前用户
        Long currentUserId = UserHolder.getUser().getId();
        //2. 查询当前用户是否关注过该用户
        Integer count = query().eq("user_id", currentUserId).eq("follow_user_id", followUserId).count();
        return Result.ok(count > 0);
    }

    @Override
    public Result commonFollows(Long targetUserId) {
        //1. 获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2. 查交集
        String key1 = "follows:" + userId;
        String key2 = "follows:" + targetUserId;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if(intersect == null || intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //3. 解析id
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //4. 查询用户
        List<UserDTO> users = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}
