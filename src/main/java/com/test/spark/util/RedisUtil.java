package com.test.spark.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;

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

    public void setValue(String key, Object data) {
        redisTemplate.delete(key);
        redisTemplate.opsForValue().set(key, data);
    }

    public void removeValue(String key) {
        redisTemplate.delete(key);
    }
}
