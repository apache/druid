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

package io.druid.client.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

public class CaffeineCacheTest
{
  private static final int RANDOM_SEED = 3478178;
  private static final byte[] HI = StringUtils.toUtf8("hiiiiiiiiiiiiiiiiiii");
  private static final byte[] HO = StringUtils.toUtf8("hooooooooooooooooooo");

  private CaffeineCache cache;
  private final CaffeineCacheConfig cacheConfig = new CaffeineCacheConfig()
  {
    @Override
    public boolean isEvictOnClose()
    {
      return true;
    }
  };

  @Before
  public void setUp()
  {
    cache = CaffeineCache.create(cacheConfig);
  }

  @Test
  public void testBasicInjection() throws Exception
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig();
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

              binder.bind(CaffeineCacheConfig.class).toInstance(config);
              binder.bind(Cache.class).toProvider(CaffeineCacheProviderWithConfig.class).in(ManageLifecycle.class);
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
    Assert.assertEquals(CaffeineCacheProvider.class, cacheProvider.getClass());
  }

  @Test
  public void testBaseOps()
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

    Assert.assertNull(cache.get(new Cache.NamedKey("miss", HI)));

    final CacheStats stats = cache.getStats();
    Assert.assertEquals(3, stats.getNumHits());
    Assert.assertEquals(5, stats.getNumMisses());
  }

  @Test
  public void testGetBulk()
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

  @Test
  public void testSizeEviction() throws Exception
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getSizeInBytes()
      {
        return 40;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final CaffeineCache cache = CaffeineCache.create(config, Runnable::run);
    forceRandomSeed(cache);

    Assert.assertNull(cache.get(key1));
    Assert.assertNull(cache.get(key2));

    cache.put(key1, val1);
    Assert.assertArrayEquals(val1, cache.get(key1));
    Assert.assertNull(cache.get(key2));

    Assert.assertEquals(0, cache.getCache().stats().evictionWeight());

    Assert.assertArrayEquals(val1, cache.get(key1));
    Assert.assertNull(cache.get(key2));

    cache.put(key2, val2);
    Assert.assertNull(cache.get(key1));
    Assert.assertArrayEquals(val2, cache.get(key2));
    Assert.assertEquals(34, cache.getCache().stats().evictionWeight());
  }

  @Test
  public void testSizeCalculation()
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getSizeInBytes()
      {
        return 40;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final Cache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());
  }

  @Test
  public void testSizeCalculationAfterDelete()
  {
    final String namespace = "the";
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getSizeInBytes()
      {
        return 999999;
      }

      @Override
      public boolean isEvictOnClose()
      {
        return true;
      }

    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey(namespace, s1);
    final Cache.NamedKey key2 = new Cache.NamedKey(namespace, s2);
    final Cache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(2L, stats.getNumEntries());
    Assert.assertEquals(68L, stats.getSizeInBytes());

    cache.close(namespace);
    stats = cache.getStats();
    Assert.assertEquals(0, stats.getNumEntries());
    Assert.assertEquals(0, stats.getSizeInBytes());
  }


  @Test
  public void testSizeCalculationMore()
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getSizeInBytes()
      {
        return 400;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final Cache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(2L, stats.getNumEntries());
    Assert.assertEquals(68L, stats.getSizeInBytes());
  }

  @Test
  public void testSizeCalculationNoWeight()
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig()
    {
      @Override
      public long getSizeInBytes()
      {
        return -1;
      }
    };
    final Random random = new Random(843671346794319L);
    final byte[] val1 = new byte[14], val2 = new byte[14];
    final byte[] s1 = new byte[]{0x01}, s2 = new byte[]{0x02};
    random.nextBytes(val1);
    random.nextBytes(val2);
    final Cache.NamedKey key1 = new Cache.NamedKey("the", s1);
    final Cache.NamedKey key2 = new Cache.NamedKey("the", s2);
    final CaffeineCache cache = CaffeineCache.create(config, Runnable::run);

    CacheStats stats = cache.getStats();
    Assert.assertEquals(0L, stats.getNumEntries());
    Assert.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assert.assertEquals(1L, stats.getNumEntries());
    Assert.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assert.assertEquals(2L, stats.getNumEntries());
    Assert.assertEquals(68L, stats.getSizeInBytes());
  }

  @Test
  public void testFromProperties()
  {
    final String keyPrefix = "cache.config.prefix";
    final Properties properties = new Properties();
    properties.put(keyPrefix + ".expireAfter", "10");
    properties.put(keyPrefix + ".sizeInBytes", "100");
    properties.put(keyPrefix + ".cacheExecutorFactory", "single_thread");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              JsonConfigProvider.bind(binder, keyPrefix, CaffeineCacheConfig.class);
            }
        )
    );
    final JsonConfigurator configurator = injector.getInstance(JsonConfigurator.class);
    final JsonConfigProvider<CaffeineCacheConfig> caffeineCacheConfigJsonConfigProvider = JsonConfigProvider.of(
        keyPrefix,
        CaffeineCacheConfig.class
    );
    caffeineCacheConfigJsonConfigProvider.inject(properties, configurator);
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get().get();
    Assert.assertEquals(10, config.getExpireAfter());
    Assert.assertEquals(100, config.getSizeInBytes());
    Assert.assertNotNull(config.createExecutor());
  }

  @Test
  public void testMixedCaseFromProperties()
  {
    final String keyPrefix = "cache.config.prefix";
    final Properties properties = new Properties();
    properties.put(keyPrefix + ".expireAfter", "10");
    properties.put(keyPrefix + ".sizeInBytes", "100");
    properties.put(keyPrefix + ".cacheExecutorFactory", "CoMmON_FjP");
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              JsonConfigProvider.bind(binder, keyPrefix, CaffeineCacheConfig.class);
            }
        )
    );
    final JsonConfigurator configurator = injector.getInstance(JsonConfigurator.class);
    final JsonConfigProvider<CaffeineCacheConfig> caffeineCacheConfigJsonConfigProvider = JsonConfigProvider.of(
        keyPrefix,
        CaffeineCacheConfig.class
    );
    caffeineCacheConfigJsonConfigProvider.inject(properties, configurator);
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get().get();
    Assert.assertEquals(10, config.getExpireAfter());
    Assert.assertEquals(100, config.getSizeInBytes());
    Assert.assertEquals(ForkJoinPool.commonPool(), config.createExecutor());
  }

  @Test
  public void testDefaultFromProperties()
  {
    final String keyPrefix = "cache.config.prefix";
    final Properties properties = new Properties();
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              JsonConfigProvider.bind(binder, keyPrefix, CaffeineCacheConfig.class);
            }
        )
    );
    final JsonConfigurator configurator = injector.getInstance(JsonConfigurator.class);
    final JsonConfigProvider<CaffeineCacheConfig> caffeineCacheConfigJsonConfigProvider = JsonConfigProvider.of(
        keyPrefix,
        CaffeineCacheConfig.class
    );
    caffeineCacheConfigJsonConfigProvider.inject(properties, configurator);
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get().get();
    Assert.assertEquals(-1, config.getExpireAfter());
    Assert.assertEquals(-1L, config.getSizeInBytes());
    Assert.assertEquals(ForkJoinPool.commonPool(), config.createExecutor());
  }

  public int get(Cache cache, Cache.NamedKey key)
  {
    return Ints.fromByteArray(cache.get(key));
  }

  public void put(Cache cache, Cache.NamedKey key, Integer value)
  {
    cache.put(key, Ints.toByteArray(value));
  }

  // See 
  public static void forceRandomSeed(CaffeineCache cache) throws Exception
  {
    final Map map = cache.getCache().asMap();
    final Method getFrequencySketch = map.getClass().getDeclaredMethod("frequencySketch");
    getFrequencySketch.setAccessible(true);
    final Object frequencySketch = getFrequencySketch.invoke(map);
    final Field seedField = frequencySketch.getClass().getDeclaredField("randomSeed");
    seedField.setAccessible(true);
    seedField.setInt(frequencySketch, RANDOM_SEED);
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
