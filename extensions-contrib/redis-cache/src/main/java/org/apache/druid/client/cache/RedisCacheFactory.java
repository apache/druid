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

import org.apache.commons.lang.StringUtils;
import org.apache.druid.java.util.common.IAE;
import redis.clients.jedis.HostAndPort;
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
    if (config.getCluster() != null && StringUtils.isNotEmpty(config.getCluster().getNodes())) {

      Set<HostAndPort> nodes = Arrays.stream(config.getCluster().getNodes().split(","))
                                     .map(String::trim)
                                     .filter(StringUtils::isNotBlank)
                                     .map(host -> {
                                       int index = host.indexOf(':');
                                       if (index <= 0 || index == host.length()) {
                                         throw new IAE("Invalid redis cluster configuration: %s", host);
                                       }

                                       int port;
                                       try {
                                         port = Integer.parseInt(host.substring(index + 1));
                                       }
                                       catch (NumberFormatException e) {
                                         throw new IAE("Invalid redis cluster configuration: invalid port %s", host);
                                       }
                                       if (port <= 0 || port > 65535) {
                                         throw new IAE("Invalid redis cluster configuration: invalid port %s", host);
                                       }

                                       return new HostAndPort(host.substring(0, index), port);
                                     }).collect(Collectors.toSet());

      JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(config.getMaxTotalConnections());
      poolConfig.setMaxIdle(config.getMaxIdleConnections());
      poolConfig.setMinIdle(config.getMinIdleConnections());

      JedisCluster cluster;
      if (org.apache.commons.lang3.StringUtils.isNotBlank(config.getPassword())) {
        cluster = new JedisCluster(
            nodes,
            config.getTimeout().getMillisecondsAsInt(),
            config.getTimeout().getMillisecondsAsInt(),
            config.getCluster().getMaxRedirection(),
            config.getPassword(),
            poolConfig
        );
      } else {
        cluster = new JedisCluster(
            nodes,
            config.getTimeout().getMillisecondsAsInt(),
            config.getCluster().getMaxRedirection(),
            poolConfig
        );
      }

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
              config.getHost(),
              config.getPort(),
              config.getTimeout().getMillisecondsAsInt(),
              config.getPassword(),
              config.getDatabase(),
              null
          ),
          config
      );
    }
  }
}
