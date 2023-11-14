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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fppt.jedismock.RedisServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class RedisStandaloneCacheTest
{
  private static final byte[] HI = StringUtils.toUtf8("hiiiiiiiiiiiiiiiiiii");
  private static final byte[] HO = StringUtils.toUtf8("hooooooooooooooooooo");

  private RedisServer server;
  private RedisStandaloneCache cache;
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

  @Before
  public void setUp() throws IOException
  {
    server = RedisServer.newRedisServer().start();
    JedisPool pool = new JedisPool(server.getHost(), server.getBindPort());
    cache = new RedisStandaloneCache(pool, cacheConfig);
  }

  @After
  public void tearDown() throws IOException
  {
    server.stop();
  }

  @Test
  public void testBasicInjection() throws Exception
  {
    String json = "{ \"host\": \"localhost\", \"port\": 6379, \"expiration\": 3600}";
    final RedisCacheConfig config = new ObjectMapper().readValue(json, RedisCacheConfig.class);

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

              binder.bindConstant().annotatedWith(Names.named("host")).to("localhost");
              binder.bindConstant().annotatedWith(Names.named("port")).to(6379);

              binder.bind(RedisCacheConfig.class).toInstance(config);
              binder.bind(Cache.class).toProvider(RedisCacheProviderWithConfig.class).in(ManageLifecycle.class);
            }
        )
    );
    Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    try {
      Cache cache = injector.getInstance(Cache.class);
      Assert.assertEquals(RedisStandaloneCache.class, cache.getClass());
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testSimpleInjection()
  {
    final String uuid = UUID.randomUUID().toString();
    System.setProperty(uuid + ".type", "redis");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

              binder.bind(Cache.class).toProvider(CacheProvider.class);
              JsonConfigProvider.bind(binder, uuid, CacheProvider.class);
            }
        )
    );
    final CacheProvider cacheProvider = injector.getInstance(CacheProvider.class);
    Assert.assertNotNull(cacheProvider);
    Assert.assertEquals(RedisCacheProvider.class, cacheProvider.getClass());
  }

  @Test
  public void testSanity()
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HI)));
    put(cache, "a", HI, 0);
    Assert.assertEquals(0, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    put(cache, "the", HI, 1);
    Assert.assertEquals(0, get(cache, "a", HI));
    Assert.assertEquals(1, get(cache, "the", HI));

    put(cache, "the", HO, 10);
    Assert.assertEquals(0, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));
    Assert.assertEquals(1, get(cache, "the", HI));
    Assert.assertEquals(10, get(cache, "the", HO));

    cache.close("the");
    Assert.assertEquals(0, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));
  }

  @Test
  public void testGetBulk()
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    put(cache, "the", HI, 1);
    put(cache, "the", HO, 10);

    Cache.NamedKey key1 = new Cache.NamedKey("the", HI);
    Cache.NamedKey key2 = new Cache.NamedKey("the", HO);
    Cache.NamedKey key3 = new Cache.NamedKey("a", HI);

    Map<Cache.NamedKey, byte[]> result = cache.getBulk(
        Lists.newArrayList(
            key1,
            key2,
            key3
        )
    );

    Assert.assertEquals(1, Ints.fromByteArray(result.get(key1)));
    Assert.assertEquals(10, Ints.fromByteArray(result.get(key2)));
    Assert.assertEquals(null, result.get(key3));
  }

  public void put(Cache cache, String namespace, byte[] key, Integer value)
  {
    cache.put(new Cache.NamedKey(namespace, key), Ints.toByteArray(value));
  }

  public int get(Cache cache, String namespace, byte[] key)
  {
    return Ints.fromByteArray(cache.get(new Cache.NamedKey(namespace, key)));
  }
}

class RedisCacheProviderWithConfig extends RedisCacheProvider
{
  private final RedisCacheConfig config;

  @Inject
  public RedisCacheProviderWithConfig(RedisCacheConfig config)
  {
    this.config = config;
  }

  @Override
  public Cache get()
  {
    return RedisCacheFactory.create(config);
  }
}

