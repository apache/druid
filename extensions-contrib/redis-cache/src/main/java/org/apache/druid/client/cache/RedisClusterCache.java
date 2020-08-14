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

import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.List;

public class RedisClusterCache extends AbstractRedisCache
{
  private JedisCluster cluster;

  RedisClusterCache(JedisCluster cluster, RedisCacheConfig config)
  {
    super(config);
    this.cluster = cluster;
  }

  @Override
  protected byte[] getFromRedis(byte[] key)
  {
    return cluster.get(key);
  }

  @Override
  protected void putToRedis(byte[] key, byte[] value, RedisCacheConfig.DurationConfig expiration)
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
    catch (IOException ignored) {
    }
  }
}
