package com.jarvis.cache.redis;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.jarvis.cache.ICacheManager;
import com.jarvis.cache.clone.ICloner;
import com.jarvis.cache.exception.CacheCenterConnectionException;
import com.jarvis.cache.serializer.ISerializer;
import com.jarvis.cache.serializer.StringSerializer;
import com.jarvis.cache.to.AutoLoadConfig;
import com.jarvis.cache.to.CacheKeyTO;
import com.jarvis.cache.to.CacheWrapper;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;

/**
 * Redis缓存管理
 * @author jiayu.qiu
 */
@Slf4j
public class JedisClusterCacheManager implements ICacheManager {

    private static final StringSerializer keySerializer=new StringSerializer();

    private final ISerializer<Object> serializer;

    private final ICloner cloner;

    private final AutoLoadConfig config;

    private JedisCluster jedisCluster;

    /**
     * Hash的缓存时长：等于0时永久缓存；大于0时，主要是为了防止一些已经不用的缓存占用内存;hashExpire小于0时，则使用@Cache中设置的expire值（默认值为-1）。
     */
    private int hashExpire=-1;

    /**
     * 是否通过脚本来设置 Hash的缓存时长
     */
    private boolean hashExpireByScript=true;

    public JedisClusterCacheManager(AutoLoadConfig config, ISerializer<Object> serializer) {
        this.config=config;
        this.serializer=serializer;
        this.cloner=serializer;
    }

    @Override
    public void setCache(final CacheKeyTO cacheKeyTO, final CacheWrapper<Object> result, final Method method, final Object args[]) throws CacheCenterConnectionException {
        if(null == jedisCluster || null == cacheKeyTO) {
            return;
        }
        String cacheKey=cacheKeyTO.getCacheKey();
        if(null == cacheKey || cacheKey.length() == 0) {
            return;
        }
        try {
            int expire=result.getExpire();
            String hfield=cacheKeyTO.getHfield();
            if(null == hfield || hfield.length() == 0) {
                if(expire == 0) {
                    jedisCluster.set(keySerializer.serialize(cacheKey), getSerializer().serialize(result));
                } else if(expire > 0) {
                    jedisCluster.setex(keySerializer.serialize(cacheKey), expire, getSerializer().serialize(result));
                }
            } else {
                hashSet(cacheKey, hfield, result);
            }
        } catch(Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
        }
    }

    private static byte[] hashSetScript;

    static {
        try {
            String tmpScript="redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);\nredis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]));";
            hashSetScript=tmpScript.getBytes("UTF-8");
        } catch(UnsupportedEncodingException ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void hashSet(String cacheKey, String hfield, CacheWrapper<Object> result) throws Exception {
        byte[] key=keySerializer.serialize(cacheKey);
        byte[] field=keySerializer.serialize(hfield);
        byte[] val=getSerializer().serialize(result);
        int hExpire;
        if(hashExpire < 0) {
            hExpire=result.getExpire();
        } else {
            hExpire=hashExpire;
        }
        if(hExpire == 0) {
            jedisCluster.hset(key, field, val);
        } else if(hExpire > 0) {
            if(hashExpireByScript) {
                List<byte[]> keys=new ArrayList<byte[]>();
                keys.add(key);

                List<byte[]> args=new ArrayList<byte[]>();
                args.add(field);
                args.add(val);
                args.add(keySerializer.serialize(String.valueOf(hExpire)));
                jedisCluster.eval(hashSetScript, keys, args);
            } else {
                jedisCluster.hset(key, field, val);
                jedisCluster.expire(key, hExpire);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public CacheWrapper<Object> get(final CacheKeyTO cacheKeyTO, final Method method, final Object args[]) throws CacheCenterConnectionException {
        if(null == jedisCluster || null == cacheKeyTO) {
            return null;
        }
        String cacheKey=cacheKeyTO.getCacheKey();
        if(null == cacheKey || cacheKey.length() == 0) {
            return null;
        }
        CacheWrapper<Object> res=null;
        try {
            byte bytes[]=null;
            String hfield=cacheKeyTO.getHfield();
            if(null == hfield || hfield.length() == 0) {
                bytes=jedisCluster.get(keySerializer.serialize(cacheKey));
            } else {
                bytes=jedisCluster.hget(keySerializer.serialize(cacheKey), keySerializer.serialize(hfield));
            }
            Type returnType=null;
            if(null != method) {
                returnType=method.getGenericReturnType();
            }
            res=(CacheWrapper<Object>)getSerializer().deserialize(bytes, returnType);
        } catch(Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
        }
        return res;
    }

    /**
     * 根据缓存Key删除缓存
     * @param cacheKeyTO 缓存Key
     */
    @Override
    public void delete(CacheKeyTO cacheKeyTO) throws CacheCenterConnectionException {
        if(null == jedisCluster || null == cacheKeyTO) {
            return;
        }
        String cacheKey=cacheKeyTO.getCacheKey();
        if(null == cacheKey || cacheKey.length() == 0) {
            return;
        }
        log.debug("delete cache:{}", cacheKey);
        try {
            String hfield=cacheKeyTO.getHfield();
            if(null == hfield || hfield.length() == 0) {
                jedisCluster.del(keySerializer.serialize(cacheKey));
            } else {
                jedisCluster.hdel(keySerializer.serialize(cacheKey), keySerializer.serialize(hfield));
            }
        } catch(Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
        }
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public void setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster=jedisCluster;
    }

    public int getHashExpire() {
        return hashExpire;
    }

    public void setHashExpire(int hashExpire) {
        if(hashExpire < 0) {
            return;
        }
        this.hashExpire=hashExpire;
    }

    public boolean isHashExpireByScript() {
        return hashExpireByScript;
    }

    public void setHashExpireByScript(boolean hashExpireByScript) {
        this.hashExpireByScript=hashExpireByScript;
    }

    @Override
    public ICloner getCloner() {
        return this.cloner;
    }

    @Override
    public ISerializer<Object> getSerializer() {
        return this.serializer;
    }

    @Override
    public AutoLoadConfig getAutoLoadConfig() {
        return this.config;
    }

}
