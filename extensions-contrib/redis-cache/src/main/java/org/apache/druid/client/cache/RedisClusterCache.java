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
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisClusterCache extends AbstractRedisCache
{
  private final JedisCluster cluster;

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

  static class Key {
    byte[] key;
    int index;

    public Key(byte[] key, int index) {
      this.key = key;
      this.index = index;
    }
  }
  @Override
  protected List<byte[]> mgetFromRedis(byte[]... keys)
  {
    if ( keys.length <= 1 ) {
      return cluster.mget(keys);
    }
    Map<Integer, List<Key>> keyGroup = new HashMap<>();
    for(int i = 0; i < keys.length; i++) {
      int slot = JedisClusterCRC16.getSlot(keys[i]);
      keyGroup.computeIfAbsent(slot, val->new ArrayList<>()).add(new Key(keys[i], i));
    }

    byte[][] returns = new byte[keys.length][];
    keyGroup.keySet().parallelStream()
            .forEach( slot-> {
                        List<Key> keyList = keyGroup.get(slot);

              List<byte[]> ret = cluster.mget(keyList.stream()
                                                     .map(key -> key.key).toArray(byte[][]::new));
              for(int i = 0; i < keyList.size(); i++) {
                returns[keyList.get(i).index] = ret.get(i);
              }
            }
            );
    return Arrays.asList(returns);
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
