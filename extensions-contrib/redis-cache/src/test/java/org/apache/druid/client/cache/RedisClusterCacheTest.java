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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fiftyonred.mock_jedis.MockJedisCluster;
import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisClusterCacheTest
{
  private static final byte[] HI = StringUtils.toUtf8("hiiiiiiiiiiiiiiiiiii");
  private static final byte[] HO = StringUtils.toUtf8("hooooooooooooooooooo");

  private final RedisCacheConfig cacheConfig = new RedisCacheConfig()
  {
    @Override
    public DurationConfig getTimeout()
    {
      return new DurationConfig("PT2S");
    }

    @Override
    public DurationConfig getExpiration()
    {
      return new DurationConfig("PT1H");
    }
  };

  private RedisClusterCache cache;

  @Before
  public void setUp()
  {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(cacheConfig.getMaxTotalConnections());
    poolConfig.setMaxIdle(cacheConfig.getMaxIdleConnections());
    poolConfig.setMinIdle(cacheConfig.getMinIdleConnections());

    // orginal MockJedisCluster does not provide full support for all public get/set interfaces
    // some methods must be overriden for test cases
    cache = new RedisClusterCache(new MockJedisCluster(Collections.singleton(new HostAndPort("localhost", 6379)))
    {
      Map<String, byte[]> cacheStorage = new HashMap<>();

      @Override
      public String setex(final byte[] key, final int seconds, final byte[] value)
      {
        cacheStorage.put(StringUtils.encodeBase64String(key), value);
        return null;
      }

      @Override
      public byte[] get(final byte[] key)
      {
        return cacheStorage.get(StringUtils.encodeBase64String(key));
      }

      @Override
      public List<byte[]> mget(final byte[]... keys)
      {
        List<byte[]> ret = new ArrayList<>();
        for (byte[] key : keys) {
          String k = StringUtils.encodeBase64String(key);
          byte[] value = cacheStorage.get(k);
          if (value != null) {
            ret.add(value);
          }
        }
        return ret;
      }
    }, cacheConfig);
  }


  @Test
  public void testConfig() throws JsonProcessingException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue("{\"expiration\": 1000}", RedisCacheConfig.class);
    Assert.assertEquals(1, fromJson.getExpiration().getSeconds());

    fromJson = mapper.readValue("{\"expiration\": \"PT1H\"}", RedisCacheConfig.class);
    Assert.assertEquals(3600, fromJson.getExpiration().getSeconds());
  }

  @Test
  public void testCache()
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    Cache.NamedKey key1 = new Cache.NamedKey("the", HI);
    Cache.NamedKey key2 = new Cache.NamedKey("the", HO);
    Cache.NamedKey key3 = new Cache.NamedKey("a", HI);

    //test put and get
    cache.put(key1, new byte[]{1, 2, 3, 4});
    cache.put(key2, new byte[]{2, 3, 4, 5});
    cache.put(key3, new byte[]{3, 4, 5, 6});
    Assert.assertEquals(0x01020304, Ints.fromByteArray(cache.get(key1)));
    Assert.assertEquals(0x02030405, Ints.fromByteArray(cache.get(key2)));
    Assert.assertEquals(0x03040506, Ints.fromByteArray(cache.get(key3)));

    //test multi get
    Map<Cache.NamedKey, byte[]> result = cache.getBulk(
        Lists.newArrayList(
            key1,
            key2,
            key3
        )
    );
    Assert.assertEquals(0x01020304, Ints.fromByteArray(result.get(key1)));
    Assert.assertEquals(0x02030405, Ints.fromByteArray(result.get(key2)));
    Assert.assertEquals(0x03040506, Ints.fromByteArray(result.get(key3)));
  }
}
