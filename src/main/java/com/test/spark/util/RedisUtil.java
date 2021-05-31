package com.test.spark.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class RedisUtil {
    private final RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public RedisUtil(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Object getValue(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public List<String> getMapValue(String key){
        return (List<String>) redisTemplate.opsForValue().get(key);
    }

    public void setValue(String key, Object data, int expire, TimeUnit timeUnit) {
        redisTemplate.delete(key);
        redisTemplate.opsForValue().set(key, data);
        redisTemplate.expire(key, expire, timeUnit);
    }

    public void removeValue(String key) {
        redisTemplate.delete(key);
    }
}
