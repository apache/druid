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

import com.google.common.collect.Maps;
import redis.clients.jedis.JedisCluster;
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

  static class Key
  {
    byte[] key;

    /**
     * index of this key in original array
     */
    int index;

    public Key(byte[] key, int index)
    {
      this.key = key;
      this.index = index;
    }
  }

  /**
   * Jedis does not work if the given keys are distributed among different redis nodes
   * A simple way is to group keys by their slots and mget values for each slot.
   * <p>
   * In the future, Jedis could be replaced by Lettuce which supports mget operation on a redis cluster
   */
  @Override
  protected List<byte[]> mgetFromRedis(byte[]... keys)
  {
    if (keys.length <= 1) {
      return cluster.mget(keys);
    }

    // group keys based on their slot
    Map<Integer, List<Key>> slot2Keys = Maps.newHashMapWithExpectedSize(keys.length);
    for (int i = 0; i < keys.length; i++) {
      int keySlot = JedisClusterCRC16.getSlot(keys[i]);
      slot2Keys.computeIfAbsent(keySlot, val -> new ArrayList<>()).add(new Key(keys[i], i));
    }

    byte[][] returning = new byte[keys.length][];
    slot2Keys.keySet()
             .parallelStream()
             .forEach(slot -> {
               List<Key> keyList = slot2Keys.get(slot);

               // mget for this slot
               List<byte[]> values = cluster.mget(keyList.stream()
                                                         .map(key -> key.key)
                                                         .toArray(byte[][]::new));

               // set returned values to their corresponding position
               for (int i = 0; i < keyList.size(); i++) {
                 returning[keyList.get(i).index] = values.get(i);
               }
             });

    return Arrays.asList(returning);
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
