package org.apache.druid.client.cache;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Author: frank.chen021@outlook.com
 * Date: 2020/7/22 2:05 下午
 */
public class RedisClusterCache extends AbstractRedisCache
{
  private JedisCluster cluster;

  RedisClusterCache(Set<HostAndPort> nodes, RedisCacheConfig config)
  {
    super(config);

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(config.getMaxTotalConnections());
    poolConfig.setMaxIdle(config.getMaxIdleConnections());
    poolConfig.setMinIdle(config.getMinIdleConnections());
    this.cluster = new JedisCluster(nodes, config.getTimeout(), config.getMaxRedirection(), poolConfig);
  }

  @Override
  protected byte[] getFromRedis(byte[] key)
  {
    return cluster.get(key);
  }

  @Override
  protected void putToRedis(byte[] key, byte[] value, Time expiration)
  {
    cluster.setex(key, (int) expiration.getSeconds(), value);
  }

  @Override
  protected List<byte[]> mgetFromRedis(byte[]... keys)
  {
    return cluster.mget(keys);
  }

  @Override
  protected void cleanup()
  {
    try {
      cluster.close();
    }
    catch (IOException e) {
    }
  }
}
