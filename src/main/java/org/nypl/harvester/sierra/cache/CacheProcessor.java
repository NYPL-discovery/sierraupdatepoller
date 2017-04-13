package org.nypl.harvester.sierra.cache;

import java.util.Map;

import org.nypl.harvester.sierra.config.EnvironmentConfig;
import org.nypl.harvester.sierra.utils.HarvesterConstants;

import redis.clients.jedis.Jedis;

public class CacheProcessor {
  private Jedis jedis = new Jedis(EnvironmentConfig.redisHost, EnvironmentConfig.redisPort);

  public void setHashAllValsInCache(String key, Map<String, String> cacheUpdateValue) {
    jedis.hmset(key, cacheUpdateValue);
    jedis.close();
  }

  public void setHashSingleValInCache(String key, String hashKey, String hashVal) {
    jedis.hset(key, hashKey, hashVal);
    jedis.close();
  }

  public Map<String, String> getHashAllValsInCache(String key) {
    return jedis.hgetAll(key);
  }

}
