package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryList() {
        String key = "typeList";
        List<String> shopTypeList = stringRedisTemplate.opsForList().range(key, 0, -1);
        if (shopTypeList.size() != 0 && shopTypeList != null){
            ArrayList<ShopType> typeList = new ArrayList<>();
            for (String shopType : shopTypeList){
                typeList.add(JSONUtil.toBean(shopType,ShopType.class));
            }
            return Result.ok(typeList);
        }
        List<ShopType> typeList = this.query().orderByAsc("sort").list();
        ArrayList<String> stringList  = new ArrayList<>();
        for (ShopType shopType : typeList) {
            stringList.add(JSONUtil.toJsonStr(shopType));
        }
        stringRedisTemplate.opsForList().rightPushAll(key, stringList);
        return Result.ok(typeList);
    }
}
