package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.constant.RedisConstants;
import com.hmdp.constant.SystemConstants;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;
    
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            //设置博客用户信息
            queryBlogUser(blog);
            //设置博客是否被当前用户点赞过
            blog.setIsLike(isLiked(blog));
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        //查询博客信息
        Blog blog = getById(id);
        //查询博客用户信息
        queryBlogUser(blog);
        //查询博客是否已被该用户点赞
        blog.setIsLike(isLiked(blog));
        return Result.ok(blog);
    }
    
    //查询并设置博客用户信息
    public void queryBlogUser(Blog blog){
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
    
    //点赞set：key为博客id，value为点赞该博客的用户id
    boolean isLiked(Blog blog){
        UserDTO currentUser = UserHolder.getUser(); //这是当前用户
        /*要注意这是首页进来会访问的接口，可能当前用户未登录，那就不用查询他的点赞信息，不用向他展示*/
        if(currentUser == null){    //用户不存在，当前博文自然不可能他被点赞过
            return false;
        }
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, currentUser.getId().toString());
        return score != null;
    }

    /**
     * 给博客点赞/取消点赞
     *  每个用户对一个博客只能点赞一次，再次点赞是取消点赞
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {
        //1. 获取当前用户id
        Long userId = UserHolder.getUser().getId();
        //2. 查询当前用户是否点赞过当前博客
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());  
        //3. 未点赞
        if(score == null){
            //3.1 修改数据库该博客点赞数+1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if(isSuccess){
                //3.2 将用户id存入redis中set集合对应博客 
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else{
            //4. 已点赞
            //4.1 修改数据库该博客点赞数-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if(isSuccess){
                //4.2 将用户id从redis中set集合移除 
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    //点赞排行榜
    @Override
    public Result queryBlogLikes(Long id) {
        //1.查询top5的点赞用户
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //2. 解析出其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",",ids);
        //3. 根据用户id查询用户 
        /*
        * 在mysql中用in(5,1)查询，他不会按你给的顺序查，这就导致我们在redis中准备的排序，到数据库一查，失效了，于是，我们要手动进行orderBy
        * WHERE id IN (5,1) ORDER BY FIELD(ID,5,1)
        * */
        List<UserDTO> userDTOS = userService.query().in("id", ids).last("ORDER BY FIELD(id,"+idStr+")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        //1. 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        //2. 保存探店笔记
        save(blog);
        //3. 查询笔记作者的所有粉丝 select * from tb_follow where follow_user_id = userId
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //4. 笔记id发送到每个粉丝的收件箱，分数是时间戳
        for(Follow follow : follows){
            //粉丝id
            Long fansId = follow.getUserId();
            //每个粉丝一个收件箱，即一个sortedSet
            String key = RedisConstants.FEED_KEY + fansId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        //3. 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询用户收件箱信息
        String key = RedisConstants.FEED_KEY + userId;
        /*最小值恒定为0，我们只用上一次查询的最小值作为当次的最大值限定范围。 数量我们写死为2。offset由后端计算返回前端，下一次查询由前端带来。
        * 所以当次的offset是前端传来的。offset和max都依赖于上一次查询，min和count都是固定值
        * */
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        //判断非空
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }
        // 3.解析收件箱数据 (key，value是博客id，score是时间戳)
        /*typedTuples里是一个一个数对，getValue()和getScore()分别取值和取分数
        *   value是博客id，score是时间戳
        * */
        List<Long> ids = new ArrayList<>(typedTuples.size());   /*arrayList扩容会影响性能，所以直接给集合的大小*/
        long minTime = 0;   /*最小时间就是集合中最后一个元素的时间（因为sortedset集合根据score排序，我们用的是reverse，所以最后一个score最小）*/
        /**/
        int os = 1; //要返回的offset （分数跟最小分数相等的元素的个数）
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            Long blogId = Long.valueOf(tuple.getValue());
            ids.add(blogId);
            long time = tuple.getScore().longValue();
            if(time == minTime){
                os++;
            }else{
                minTime = time;
                os = 1;
            }
        }
        // 4.通过id查询博客信息 select * from tb_blog where id in (ids) order by FIELD('id',idStr);
        String idsStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id,"+ idsStr +")").list();
        // 5.查询封装博客用户、点赞信息
        for(Blog blog : blogs){
            queryBlogUser(blog);
            blog.setIsLike(isLiked(blog));
        }
        // 6.封装返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setMinTime(minTime);
        r.setOffset(os);
        return Result.ok(r);
    }
}
