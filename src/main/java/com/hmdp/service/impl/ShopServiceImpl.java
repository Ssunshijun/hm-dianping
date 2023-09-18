package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * 查店铺详细信息
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

        //互斥锁--解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期--解决缓存击穿
//        Shop shop = cacheClient.queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }


    /**
     * 测试方法：模拟实际管理平台进行，缓存预热
     *
     * @param id            商铺id
     * @param expireSeconds 逻辑过期时间
     */
    public void saveShop2Redis(Long id, Long expireSeconds) {
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();   //逻辑过期中间类
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }



/*    //缓存击穿-逻辑过期
    private static final ExecutorService CACHE_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查数据
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(shopJson)) {  //redis无
            return null;
        }
        //有，json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject)redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期，
        if (expireTime.isAfter(LocalDateTime.now())){
            //未过期
            return shop;
        }
        //过期，缓存重建。 获取互斥锁。。。
        String lockKey = LOCK_SHOP_KEY + shop;
        boolean isLock = tryLock(lockKey);
        //获取锁成功，，，开启独立线程，重建
        if (isLock){
            CACHE_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id,30*60L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unlock(lockKey);
                }

            });
        }
        //获取锁失败
        return shop;
    }
*/

    //击穿-互斥锁
/*    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查数据
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {  //redis有
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否为“”,缓存穿透保存空
        if (shopJson != null) return null;


        //redis无，从数据库查数据
        //缓存重建——缓存击穿

        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);//获取锁
            if (!isLock){  //获取互斥锁失败
                Thread.sleep(50);
                return queryWithMutex(id);//递归，重试获取锁

            }
            shop = getById(id);
            Thread.sleep(200);//模拟重建延时
            //2.数据库无
            if (shop == null) {  //缓存穿透解决之一，缓存空
                stringRedisTemplate.opsForValue().set(key, "",CACHE_SHOP_TTL, TimeUnit.MINUTES);
            }
            //数据库有，写入redis。设置TTL，属内存更新的超时剔除
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_NULL_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放互斥锁
            unlock(lockKey);
        }

        //结果返回
        return shop;
    }
*/

    //穿透
 /*   public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查数据
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {  //redis有
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否为“”,缓存穿透解决之一
        if(shopJson != null) return null;

        //redis无，从数据库查数据
        Shop shop = getById(id);
        //2.数据库无
        if (shop == null) {  //缓存穿透解决之一，缓存空
            stringRedisTemplate.opsForValue().set(key, "",CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }
        //数据库有，写入redis。设置TTL，属内存更新的超时剔除
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_NULL_TTL, TimeUnit.MINUTES);
        //结果返回
        return shop;
    }
*/

    //获取互斥锁方法
/*
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
*/


    //更新商铺信息
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) return Result.fail("店铺id不能为空!");
        //1.先更新数据库
        updateById(shop);
        //2.后删除redis缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y, String sortMethod) {
        LambdaQueryWrapper<Shop> shopLambdaQueryWrapper = new LambdaQueryWrapper<>();
        // 是否需要根据坐标查询
        if (x == null || y == null){
            shopLambdaQueryWrapper.eq(Shop::getTypeId, typeId);
            Page<Shop> page = page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE), shopLambdaQueryWrapper);
            return Result.ok(page.getRecords());
        }
//        if (sortMethod.equals("comments")){
//            shopLambdaQueryWrapper.eq(Shop::getTypeId, typeId).orderByDesc(Shop::getComments);
//            Page<Shop> page = page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE), shopLambdaQueryWrapper);
//            return Result.ok(page.getRecords());
//        }
//        if (sortMethod.equals("score")){
//            shopLambdaQueryWrapper.eq(Shop::getTypeId, typeId).orderByDesc(Shop::getScore);
//            Page<Shop> page = page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE), shopLambdaQueryWrapper);
//            return Result.ok(page.getRecords());
//        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        String key = SHOP_GEO_KEY + typeId;
        // 查询redis、按距离排序、shopId、distance
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .radius(key, new Circle(new Point(x, y), new Distance(5000)),
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(end));
        if (results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> contents = results.getContent();
        if (contents.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(contents.size());
        Map<String, Distance> map = new HashMap<>(contents.size());
        contents.stream().skip(from).forEach(result -> {
            // 获取店铺id
            String shopId = result.getContent().getName();
            ids.add(Long.valueOf(shopId));
            // 获取距离
            Distance distance = result.getDistance();
            map.put(shopId, distance);
        });
        // 根据id查询shop
        String idStr = StrUtil.join(",", ids);
        System.out.println("idStr = " + idStr);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD (id, " + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(map.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }

}

