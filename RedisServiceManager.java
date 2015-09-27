package com.bsb.portal.db;

import com.bsb.portal.config.RedisConfig;

import redis.clients.jedis.*;

import java.util.*;

/**
 * Created with IntelliJ IDEA. User: bhuvangupta Date: 25/09/12 Time: 9:18 PM To change this
 * template use File | Settings | File Templates.
 */
public class RedisServiceManager {

    private JedisPool redisPool;

    public static final int DEFAULT_TIMEOUT = 5000;

    public RedisServiceManager(RedisConfig config) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getMaxActive());
        poolConfig.setMaxIdle(config.getMaxIdle());
        redisPool = new JedisPool(poolConfig, config.getRedisHost(), config.getRedisPort(), DEFAULT_TIMEOUT, null, config.getRedisDB());
    }
    
    public void shutdown() {
        redisPool.destroy();
    }

    public Set<String> hkeys(String key) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.hkeys(key);
        }
        catch (Exception e) {
            return null;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public List<String> hvals(String field) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.hvals(field);
        }
        catch (Exception e) {
            return null;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public List<String> hmget(String key, String[] fields) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            List<String> values = redis.hmget(key, fields);
            return values;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public void hmset(String key, Map<String, String> fields) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            redis.hmset(key, fields);
            return;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }


    public Set<String> keys(String keyPattern) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            Set<String> values = redis.keys(keyPattern);
            return values;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public String hget(String key, String field) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            String val = redis.hget(key, field);
            // if(field.endsWith("/s"))
            // System.out.println(System.currentTimeMillis()+" - Using REDISS connection : "+redis+" to get "+val+" for "+field+"."+Thread.currentThread().toString());
            return val;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Map<String, String> hgetAll(String key) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.hgetAll(key);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public void hrem(String key, String field) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            redis.hdel(key, field);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Long hset(String key, String field, String value) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.hset(key, field, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }


    public Long hdel(String key, String field) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.hdel(key, field);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Long hsetnx(String key, String field, String value) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.hsetnx(key, field, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    /**
     * Returns number of entries in a given hashmap
     *
     * @param mapName
     * @return
     */
    public Long hlen(String mapName) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.hlen(mapName);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public Long lpush(String key, String value)
    {
        Jedis redis = null;
        try
        {
            redis = redisPool.getResource();
            return redis.lpush(key, value);
        }
        finally
        {
            if (redis != null)
            {
                redisPool.returnResource(redis);
            }
        }
    }

    public Long lrem(String key, int count,String value)
    {
        Jedis redis = null;
        try
        {
            redis = redisPool.getResource();
            return redis.lrem(key,count, value);
        }
        finally
        {
            if (redis != null)
            {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public List<String> lrange(String key, long start,long end)
    {
        Jedis redis = null;
        try
        {
            redis = redisPool.getResource();
            return redis.lrange(key, start, end);
        }
        finally
        {
            if (redis != null)
            {
                redisPool.returnResource(redis);
            }
        }
    }

    public long sadd(String key, String value){
    	Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.sadd(key, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    public long srem(String key, String value){
    	Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.srem(key, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public long saddWithExpire(String key, String value,int expireAfter){
    	Jedis redis = null;
        try {
            redis = redisPool.getResource();
            long ret = redis.sadd(key, value);
            redis.expire(key, expireAfter);
            return ret;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public void hclear(String key) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            Set<String> fields = redis.hkeys(key);
            for (Iterator<String> fieldsItr = fields.iterator(); fieldsItr.hasNext(); ) {
                String field = fieldsItr.next();
                redis.hdel(key, field);
            }

        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public boolean hexists(String key, String field) {
    	Jedis redis = null;
    	try {
    		redis = redisPool.getResource();
    		return redis.hexists(key, field);
    	}
    	finally {
    		if(redis != null) {
    			redisPool.returnResource(redis);
    		}
    	}
    }

    public String setex(String key, String value, int keyExpiryInSeconds) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.setex(key, keyExpiryInSeconds, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public String set(String key, String value) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.set(key, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public boolean sismember(String key, String value) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.sismember(key, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public String srandmember(String key) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.srandmember(key);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Set<String> smembers(String key) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.smembers(key);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public String get(String key) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            String val = redis.get(key);
            return val;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public String getSet(String key, String value) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            String val = redis.getSet(key, value);
            return val;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public List<String> mget(List<String> keys) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            if(keys==null){
                return null;
            }
            List<String> vals = redis.mget(keys.toArray(new String[0]));
            return vals;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public long expire(String key, int seconds) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.expire(key, seconds);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public long zadd(String key, String value , double sortingParam){
    	Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.zadd(key, sortingParam, value);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public long zremrangeByRank(String key, long start , long end){
    	Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.zremrangeByRank(key, start, end);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public Set<String> zrange(String key, long start, long stop) {
    	Jedis redis = null;
    	try {
    		redis = redisPool.getResource();
    		return redis.zrange(key, start, stop);
    	}
    	finally {
    		if(redis != null) {
    			redisPool.returnResource(redis);
    		}
    	}
    }

    public long zrem(String key, String member) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.zrem(key, member);
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Double zscore(String key, String member) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.zscore(key, member);
        } finally {
            if(redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Map<String, Double> zscan(String key, String match, Integer count) {
        Jedis redis = null;
        try {
            Map<String, Double> map = new HashMap<String, Double>();
            redis = redisPool.getResource();
            ScanParams params = new ScanParams();
            params.count(count);
            params.match(match);
            ScanResult<Tuple> result = null;
            do {
                if(result == null)
                    result = redis.zscan(key, "0", params);
                else
                    result = redis.zscan(key, result.getStringCursor(), params);
                List<Tuple> listResult = result.getResult();
                for(Tuple tuple : listResult) {
                    map.put(tuple.getElement(), tuple.getScore());
                }
            } while(!result.getStringCursor().equals("0"));
            return map;
        } finally {
            if(redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public Long incr(String key){
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            Long val = redis.incr(key);
            return val;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }


    public Long hincrBy(String hash,String key,int value){
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            Long val = redis.hincrBy(hash,key,value);
            return val;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Long zadd(String key,double score,String member)
    {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            Long val = redis.zadd(key,score,member);
            return val;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public double zincrby(String key,double score,String member)
    {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            double val = redis.zincrby(key,score,member);
            return val;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }

    public Set<String> zrevrange(String key, long start, long stop) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            return redis.zrevrange(key, start, stop);
        }
        finally {
            if(redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public Long delete(String key) {
        Jedis redis = null;
        try {
            redis = redisPool.getResource();
            Long del = redis.del(key);
            return del;
        }
        finally {
            if (redis != null) {
                redisPool.returnResource(redis);
            }
        }
    }
    
    public String ltrim(String key, long start, long end)
    {
        Jedis redis = null;
        try
        {
            redis = redisPool.getResource();
            return redis.ltrim(key, start, end);
        }
        finally
        {
            if (redis != null)
            {
                redisPool.returnResource(redis);
            }
        }
    }

    public Long publish(String channelName,String message)
    {
        Jedis redis = null;
        try
        {
            redis = redisPool.getResource();
            return redis.publish(channelName,message);
        }
        finally
        {
            if (redis != null)
            {
                redisPool.returnResource(redis);
            }
        }
    }

    public void subscribe(String channelName,JedisPubSub pubsub)
    {
        Jedis redis = null;
        try
        {
            redis = redisPool.getResource();
            redis.subscribe(pubsub,channelName);
        }
        finally
        {
            if (redis != null)
            {
                redisPool.returnResource(redis);
            }
        }
    }
    
}
