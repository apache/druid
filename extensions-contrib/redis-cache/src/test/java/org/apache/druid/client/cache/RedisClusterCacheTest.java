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
import com.github.fppt.jedismock.RedisServer;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
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

  private RedisServer server;
  private RedisClusterCache cache;

  @Before
  public void setUp() throws IOException
  {
    ServiceOptions options = ServiceOptions.defaultOptions().withClusterModeEnabled();
    server = RedisServer.newRedisServer().setOptions(options).start();
    HostAndPort hostAndPort = new HostAndPort(server.getHost(), server.getBindPort());
    JedisCluster cluster = new JedisCluster(hostAndPort);
    cache = new RedisClusterCache(cluster, cacheConfig);
  }

  @After
  public void tearDown() throws IOException
  {
    server.stop();
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
    Cache.NamedKey notExist = new Cache.NamedKey("notExist", HI);

    //test put and get
    cache.put(key1, new byte[]{1, 2, 3, 4});
    cache.put(key2, new byte[]{2, 3, 4, 5});
    cache.put(key3, new byte[]{3, 4, 5, 6});
    Assert.assertEquals(0x01020304, Ints.fromByteArray(cache.get(key1)));
    Assert.assertEquals(0x02030405, Ints.fromByteArray(cache.get(key2)));
    Assert.assertEquals(0x03040506, Ints.fromByteArray(cache.get(key3)));
    Assert.assertEquals(0x03040506, Ints.fromByteArray(cache.get(key3)));
    Assert.assertNull(cache.get(notExist));

    //test multi get
    Map<Cache.NamedKey, byte[]> result = cache.getBulk(
        Lists.newArrayList(
            key1,
            key2,
            key3,
            notExist
        )
    );

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(0x01020304, Ints.fromByteArray(result.get(key1)));
    Assert.assertEquals(0x02030405, Ints.fromByteArray(result.get(key2)));
    Assert.assertEquals(0x03040506, Ints.fromByteArray(result.get(key3)));
  }
}
