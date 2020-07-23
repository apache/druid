package org.apache.druid.client.cache;

import org.apache.commons.lang.StringUtils;
import org.apache.druid.java.util.common.IAE;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Author: frank.chen021@outlook.com
 * Date: 2020/7/22 4:18 下午
 */
public class RedisCacheFactory
{
  public static Cache create(final RedisCacheConfig config)
  {
    if (StringUtils.isNotEmpty(config.getCluster())) {

      Set<HostAndPort> nodes = Arrays.stream(config.getCluster().split(","))
                                     .map(host -> host.trim())
                                     .filter(host -> StringUtils.isNotBlank(host))
                                     .map(host -> {
                                       int index = host.indexOf(':');
                                       if (index <= 0 || index == host.length() ) {
                                         throw new IAE("invalid redis cluster configuration: %s", host);
                                       }

                                       int port = -1;
                                       try {
                                         port = Integer.parseInt(host.substring(index + 1));
                                       }
                                       catch (NumberFormatException e) {
                                         throw new IAE("invalid redis cluster configuration: invalid port %s", host);
                                       }
                                       if (port <= 0 || port > 65535) {
                                         throw new IAE("invalid redis cluster configuration: invalid port %s", host);
                                       }

                                       return new HostAndPort(host.substring(0, index), port);
                                     }).collect(Collectors.toSet());

      return new RedisClusterCache(nodes, config);

    } else {

      if ( StringUtils.isEmpty(config.getHost()) ) {
        throw new IAE("invalid redis configuration. no redis server or cluster configured.");
      }

      JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(config.getMaxTotalConnections());
      poolConfig.setMaxIdle(config.getMaxIdleConnections());
      poolConfig.setMinIdle(config.getMinIdleConnections());

      return new RedisSingleNodeCache(new JedisPool(poolConfig,
                                                    config.getHost(),
                                                    config.getPort(),
                                                    config.getTimeout()),
                                      config);
    }
  }
}
