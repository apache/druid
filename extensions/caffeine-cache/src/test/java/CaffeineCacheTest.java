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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import io.druid.client.cache.CaffeineCache;
import io.druid.client.cache.CaffeineCacheConfig;
import io.druid.client.cache.CaffeineCacheProvider;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.Initialization;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

public class CaffeineCacheTest
{
  private static final byte[] HI = "hiiiiiiiiiiiiiiiiiii".getBytes();
  private static final byte[] HO = "hooooooooooooooooooo".getBytes();

  private CaffeineCache cache;
  private final CaffeineCacheConfig cacheConfig = new CaffeineCacheConfig()
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
  };

  @Before
  public void setUp() throws Exception
  {
    cache = CaffeineCache.create(cacheConfig);
  }

  @Test
  public void testBasicInjection() throws Exception
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig();
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);

                binder.bind(CaffeineCacheConfig.class).toInstance(config);
                binder.bind(Cache.class).toProvider(CaffeineCacheProviderWithConfig.class).in(ManageLifecycle.class);
              }
            }
        )
    );
    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    try {
      Cache cache = injector.getInstance(Cache.class);
      Assert.assertEquals(CaffeineCache.class, cache.getClass());
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testSimpleInjection()
  {
    final String uuid = UUID.randomUUID().toString();
    System.setProperty(uuid + ".type", "caffeine");
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
    final CacheProvider cacheProvider = injector.getInstance(CacheProvider.class);
    Assert.assertNotNull(cacheProvider);
    Assert.assertEquals(CaffeineCacheProvider.class, cacheProvider.getClass());
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

  public void put(Cache cache, Cache.NamedKey key, Integer value)
  {
    cache.put(key, Ints.toByteArray(value));
  }
}

class CaffeineCacheProviderWithConfig extends CaffeineCacheProvider
{
  private final CaffeineCacheConfig config;

  @Inject
  public CaffeineCacheProviderWithConfig(CaffeineCacheConfig config)
  {
    this.config = config;
  }

  @Override
  public Cache get()
  {
    return CaffeineCache.create(config);
  }
}
