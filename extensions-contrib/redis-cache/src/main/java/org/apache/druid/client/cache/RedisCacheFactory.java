/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client.cache;

import org.apache.commons.lang3.StringUtils;
import org.apache.druid.java.util.common.IAE;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisCacheFactory
{
  public static Cache create(final RedisCacheConfig config)
  {
    if (config.getCluster() != null && StringUtils.isNotBlank(config.getCluster().getNodes())) {

      Set<HostAndPort> nodes = Arrays.stream(config.getCluster().getNodes().split(","))
                                     .map(String::trim)
                                     .filter(StringUtils::isNotBlank)
                                     .map(hostAndPort -> {
                                       int index = hostAndPort.indexOf(':');
                                       if (index <= 0 || index == hostAndPort.length()) {
                                         throw new IAE("Invalid redis cluster configuration: %s", hostAndPort);
                                       }

                                       int port;
                                       try {
                                         port = Integer.parseInt(hostAndPort.substring(index + 1));
                                       }
                                       catch (NumberFormatException e) {
                                         throw new IAE("Invalid port in %s", hostAndPort);
                                       }
                                       if (port <= 0 || port > 65535) {
                                         throw new IAE("Invalid port in %s", hostAndPort);
                                       }

                                       return new HostAndPort(hostAndPort.substring(0, index), port);
                                     }).collect(Collectors.toSet());

      ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
      poolConfig.setMaxTotal(config.getMaxTotalConnections());
      poolConfig.setMaxIdle(config.getMaxIdleConnections());
      poolConfig.setMinIdle(config.getMinIdleConnections());

      JedisCluster cluster = new JedisCluster(
          nodes,
          buildClientConfig(config),
          config.getCluster().getMaxRedirection(),
          poolConfig
      );

      return new RedisClusterCache(cluster, config);

    } else {

      if (StringUtils.isBlank(config.getHost())) {
        throw new IAE("Invalid redis configuration. no redis server or cluster configured.");
      }

      JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(config.getMaxTotalConnections());
      poolConfig.setMaxIdle(config.getMaxIdleConnections());
      poolConfig.setMinIdle(config.getMinIdleConnections());

      return new RedisStandaloneCache(
          new JedisPool(
              poolConfig,
              new HostAndPort(config.getHost(), config.getPort()),
              buildClientConfig(config)
          ),
          config
      );
    }
  }

  private static JedisClientConfig buildClientConfig(RedisCacheConfig config)
  {
    return DefaultJedisClientConfig
        .builder()
        .connectionTimeoutMillis(config.getTimeout().getMillisecondsAsInt())
        .socketTimeoutMillis(config.getTimeout().getMillisecondsAsInt())
        .password(config.getPassword() == null ? null : config.getPassword().getPassword())
        // database applies to standalone only; Redis Cluster supports database 0 only, so this
        // is a no-op in cluster mode (the default is 0).
        .database(config.getDatabase())
        .ssl(config.getEnableTls())
        .build();
  }
}
