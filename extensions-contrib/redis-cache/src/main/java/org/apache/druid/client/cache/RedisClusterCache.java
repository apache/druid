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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  static class CachableKey
  {
    byte[] keyBytes;
    NamedKey namedKey;

    public CachableKey(NamedKey namedKey)
    {
      this.keyBytes = namedKey.toByteArray();
      this.namedKey = namedKey;
    }
  }

  /**
   * Jedis does not work if the given keys are distributed among different redis nodes
   * A simple workaround is to group keys by their slots and mget values for each slot.
   * <p>
   * In future, Jedis could be replaced by the Lettuce driver which supports mget operation on a redis cluster
   */
  @Override
  protected Map<NamedKey, byte[]> mgetFromRedis(Iterable<NamedKey> keys)
  {
    // group keys based on their slot
    Map<Integer, List<CachableKey>> slot2Keys = new HashMap<>();
    for (NamedKey key : keys) {
      CachableKey cachableKey = new CachableKey(key);
      int keySlot = JedisClusterCRC16.getSlot(cachableKey.keyBytes);
      slot2Keys.computeIfAbsent(keySlot, val -> new ArrayList<>()).add(cachableKey);
    }

    Map<NamedKey, byte[]> results = new ConcurrentHashMap<>();
    slot2Keys.keySet()
             .parallelStream()
             .forEach(slot -> {
               List<CachableKey> keyList = slot2Keys.get(slot);

               // mget for this slot
               List<byte[]> values = cluster.mget(keyList.stream()
                                                         .map(key -> key.keyBytes)
                                                         .toArray(byte[][]::new));

               for (int i = 0; i < keyList.size(); i++) {
                 byte[] value = values.get(i);
                 if (value != null) {
                   results.put(keyList.get(i).namedKey, value);
                 }
               }
             });

    return results;
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
