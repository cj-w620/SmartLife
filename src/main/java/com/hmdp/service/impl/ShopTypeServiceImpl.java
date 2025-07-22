package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.constant.RedisConstants;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 查询所有分类
     * @return
     */
    @Override
    public List<ShopType> queryTypeList() {
        String typeKey = RedisConstants.CACHE_SHOE_TYPE_KEY;
        //1.redis中查询分类数据
        String typeListJson = stringRedisTemplate.opsForValue().get(typeKey);
        //2.1 存在，直接返回
        if(StrUtil.isNotBlank(typeListJson)){
            return JSONUtil.toList(typeListJson, ShopType.class);
        }
        //2.2 不存在
        //3. 去数据库查询分类数据
        List<ShopType> typeList = query().orderByAsc("sort").list();
        
        //4. 分类数据存入redis
        stringRedisTemplate.opsForValue().set(typeKey,JSONUtil.toJsonStr(typeList));
        //5. 返回分类数据
        return typeList;
    }
}
