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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStandaloneCache extends AbstractRedisCache
{
  private final JedisPool pool;

  RedisStandaloneCache(JedisPool pool, RedisCacheConfig config)
  {
    super(config);

    this.pool = pool;
  }

  @Override
  protected byte[] getFromRedis(byte[] key)
  {
    try (Jedis jedis = pool.getResource()) {
      return jedis.get(key);
    }
  }

  @Override
  protected void putToRedis(byte[] key, byte[] value, RedisCacheConfig.DurationConfig expiration)
  {
    try (Jedis jedis = pool.getResource()) {
      jedis.psetex(key, expiration.getMilliseconds(), value);
    }
  }

  @Override
  protected Pair<Integer, Map<NamedKey, byte[]>> mgetFromRedis(Iterable<NamedKey> keys)
  {
    List<NamedKey> namedKeys = Lists.newArrayList(keys);
    List<byte[]> byteKeys = Lists.transform(namedKeys, NamedKey::toByteArray);

    try (Jedis jedis = pool.getResource()) {

      List<byte[]> byteValues = jedis.mget(byteKeys.toArray(new byte[0][]));

      Map<NamedKey, byte[]> results = new HashMap<>();
      for (int i = 0; i < byteValues.size(); ++i) {
        if (byteValues.get(i) != null) {
          results.put(namedKeys.get(i), byteValues.get(i));
        }
      }

      return new Pair<>(namedKeys.size(), results);
    }
  }

  @Override
  protected void cleanup()
  {
    pool.close();
  }
}
