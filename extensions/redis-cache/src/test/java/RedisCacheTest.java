/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheProvider;
import io.druid.client.cache.CacheStats;
import io.druid.client.cache.RedisCache;
import io.druid.client.cache.RedisCacheConfig;
import io.druid.client.cache.RedisCacheProvider;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.Initialization;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RedisCacheTest
{
  private static final byte[] HI = "hiiiiiiiiiiiiiiiiiii".getBytes();
  private static final byte[] HO = "hooooooooooooooooooo".getBytes();

  private RedisCache cache;
  private final RedisCacheConfig cacheConfig = new RedisCacheConfig()
  {
    public int getPoolSize()
    {
      return 1;
    }

    public String getPrefix()
    {
      return "druid";
    }

    public long getExpiration()
    {
      return -1;
    }

    public List<JedisShardInfo> getShardsInfo()
    {
      return new ArrayList<>();
    }
  };

  @Before
  public void setUp() throws Exception
  {
    cache = new RedisCache(
        Suppliers.<ResourceHolder<ShardedJedis>>ofInstance(
            StupidResourceHolder.<ShardedJedis>create(new MockRedisClient())
        ),
        cacheConfig
    );
  }

  @Test
  public void testBasicInjection() throws Exception
  {
    final RedisCacheConfig config = new RedisCacheConfig()
    {
      @Override
      public List<JedisShardInfo> getShardsInfo()
      {
        return new ArrayList<>();
      }
    };
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);

                binder.bind(RedisCacheConfig.class).toInstance(config);
                binder.bind(Cache.class).toProvider(RedisCacheProviderWithConfig.class).in(ManageLifecycle.class);
              }
            }
        )
    );
    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    try {
      Cache cache = injector.getInstance(Cache.class);
      Assert.assertEquals(RedisCache.class, cache.getClass());
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
    System.setProperty(uuid + ".hosts", "[\"localhost\"]");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);

                binder.bind(Cache.class).toProvider(CacheProvider.class);
                JsonConfigProvider.bind(binder, uuid, CacheProvider.class);
              }
            }
        )
    );
    final CacheProvider redisCacheProvider = injector.getInstance(CacheProvider.class);
    Assert.assertNotNull(redisCacheProvider);
    Assert.assertEquals(RedisCacheProvider.class, redisCacheProvider.getClass());
  }

  @Test
  public void testBaseOps() throws Exception
  {
    final Cache.NamedKey aKey = new Cache.NamedKey("a", HI);
    Assert.assertNull(cache.get(aKey));
    put(cache, aKey, 1);
    Assert.assertEquals(1, get(cache, aKey));
    cache.close("a");
    Assert.assertNull(cache.get(aKey));

    final Cache.NamedKey hiKey = new Cache.NamedKey("the", HI);
    final Cache.NamedKey hoKey = new Cache.NamedKey("the", HO);
    put(cache, hiKey, 10);
    put(cache, hoKey, 20);
    Assert.assertEquals(10, get(cache, hiKey));
    Assert.assertEquals(20, get(cache, hoKey));
    cache.close("the");
    Assert.assertNull(cache.get(hiKey));
    Assert.assertNull(cache.get(hoKey));

    final CacheStats stats = cache.getStats();
    Assert.assertEquals(3, stats.getNumHits());
    Assert.assertEquals(4, stats.getNumMisses());
  }

  @Test
  public void testGetBulk() throws Exception
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    Cache.NamedKey key1 = new Cache.NamedKey("the", HI);
    put(cache, key1, 2);

    Cache.NamedKey key2 = new Cache.NamedKey("the", HO);
    put(cache, key2, 10);

    Map<Cache.NamedKey, byte[]> result = cache.getBulk(
        Lists.newArrayList(
            key1,
            key2
        )
    );

    Assert.assertEquals(2, Ints.fromByteArray(result.get(key1)));
    Assert.assertEquals(10, Ints.fromByteArray(result.get(key2)));

    Cache.NamedKey missingKey = new Cache.NamedKey("missing", HI);
    result = cache.getBulk(Lists.newArrayList(missingKey));
    Assert.assertEquals(result.size(), 0);

    result = cache.getBulk(Lists.<Cache.NamedKey>newArrayList());
    Assert.assertEquals(result.size(), 0);
  }

  public int get(Cache cache, Cache.NamedKey key)
  {
    return Ints.fromByteArray(cache.get(key));
  }

  public void put(Cache cache, Cache.NamedKey key, Integer value) {
    cache.put(key, Ints.toByteArray(value));
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
    return RedisCache.create(config);
  }
}


@SuppressWarnings("unchecked")
class MockRedisClient extends ShardedJedis
{
  private final Map keyMap = new HashMap();

  public MockRedisClient()
  {
    super(new ArrayList<JedisShardInfo>());
  }

  @Override
  public Long del(byte[] key)
  {
    final String hex = Hex.encodeHexString(key);
    keyMap.remove(hex);
    return (long) 1;
  }

  @Override
  public Long del(String key)
  {
    final String hex = Hex.encodeHexString(key.getBytes());
    keyMap.remove(hex);
    return (long) 1;
  }

  @Override
  public byte[] get(byte[] key)
  {
    final String hex = Hex.encodeHexString(key);
    return (byte[]) keyMap.get(hex);
  }

  @Override
  public Jedis getShard(byte[] key)
  {
    return new Jedis()
    {
      @Override
      public byte[] get(byte[] key)
      {
        return MockRedisClient.this.get(key);
      }

      @Override
      public String psetex(byte[] key, long milliseconds, byte[] value)
      {
        return MockRedisClient.this.set(key, value);
      }

      @Override
      public Long sadd(byte[] key, byte[]... members)
      {
        return MockRedisClient.this.sadd(key, members);
      }

      @Override
      public String set(byte[] key, byte[] value)
      {
        return MockRedisClient.this.set(key, value);
      }
    };
  }

  @Override
  public ShardedJedisPipeline pipelined() {
    return new MockPipeline(this);
  }

  @Override
  public Long sadd(byte[] key, byte[]... members)
  {
    final String hex = Hex.encodeHexString(key);
    final Object value = keyMap.get(hex);

    if (value == null) {
      final HashSet<byte[]> set = Sets.newHashSet(members);
      keyMap.put(hex, set);
      return (long) set.size();
    }

    final HashSet<byte[]> set = (HashSet<byte[]>) value;
    long added = 0;
    for (byte[] m : members) {
      if (set.add(m)) {
        ++added;
      }
    }

    return added;
  }

  @Override
  public String set(byte[] key, byte[] value)
  {
    final String hex = Hex.encodeHexString(key);
    keyMap.put(hex, value);
    return null;
  }

  @Override
  public Set<byte[]> smembers(byte[] key)
  {
    final String hex = Hex.encodeHexString(key);
    Object value = keyMap.get(hex);
    if (value == null) {
      return new HashSet<>();
    }

    return (HashSet<byte[]>) value;
  }
}

class MockPipeline extends ShardedJedisPipeline {
  private final List<Object> results = new ArrayList<>();
  private final ShardedJedis jedis;

  public MockPipeline(ShardedJedis jedis) {
    this.jedis = jedis;
  }

  @Override
  public Response<byte[]> get(byte[] key)
  {
    results.add(jedis.get(key));
    return new MockResponse();
  }

  @Override
  public List<Object> getResults() {
    return results;
  }

  @Override
  public void sync()
  {
    // do nothing here
  }
}

class MockResponse extends Response<byte[]> {
  public MockResponse() {
    super(null);
  }
}
