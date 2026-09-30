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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

public class CaffeineCacheTest extends CacheTestBase<CaffeineCache>
{
  private static final byte[] HI = StringUtils.toUtf8("hiiiiiiiiiiiiiiiiiii");
  private static final byte[] HO = StringUtils.toUtf8("hooooooooooooooooooo");

  private final CaffeineCacheConfig cacheConfig = new CaffeineCacheConfig()
  {
    @Override
    public boolean isEvictOnClose()
    {
      return true;
    }
  };

  @BeforeEach
  public void setUp()
  {
    cache = CaffeineCache.create(cacheConfig);
  }

  @Test
  public void testBasicInjection() throws Exception
  {
    final CaffeineCacheConfig config = new CaffeineCacheConfig();
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
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
      Assertions.assertEquals(CaffeineCache.class, cache.getClass());
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
    Assertions.assertNotNull(cacheProvider);
    Assertions.assertEquals(CaffeineCacheProvider.class, cacheProvider.getClass());
  }

  @Test
  public void testBaseOps()
  {
    final Cache.NamedKey aKey = new Cache.NamedKey("a", HI);
    Assertions.assertNull(cache.get(aKey));
    put(cache, aKey, 1);
    Assertions.assertEquals(1, get(cache, aKey));

    cache.close("a");
    Assertions.assertNull(cache.get(aKey));

    final Cache.NamedKey hiKey = new Cache.NamedKey("the", HI);
    final Cache.NamedKey hoKey = new Cache.NamedKey("the", HO);
    put(cache, hiKey, 10);
    put(cache, hoKey, 20);
    Assertions.assertEquals(10, get(cache, hiKey));
    Assertions.assertEquals(20, get(cache, hoKey));
    cache.close("the");

    Assertions.assertNull(cache.get(hiKey));
    Assertions.assertNull(cache.get(hoKey));

    Assertions.assertNull(cache.get(new Cache.NamedKey("miss", HI)));

    final CacheStats stats = cache.getStats();
    Assertions.assertEquals(3, stats.getNumHits());
    Assertions.assertEquals(5, stats.getNumMisses());
  }

  @Test
  public void testGetBulk()
  {
    Assertions.assertNull(cache.get(new Cache.NamedKey("the", HI)));

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

    Assertions.assertEquals(2, Ints.fromByteArray(result.get(key1)));
    Assertions.assertEquals(10, Ints.fromByteArray(result.get(key2)));

    Cache.NamedKey missingKey = new Cache.NamedKey("missing", HI);
    result = cache.getBulk(Collections.singletonList(missingKey));
    Assertions.assertEquals(result.size(), 0);

    result = cache.getBulk(new ArrayList<>());
    Assertions.assertEquals(result.size(), 0);
  }

  @Test
  public void testSizeEviction()
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

    Assertions.assertEquals(0, cache.getCache().stats().evictionWeight());

    // Two entries with combined weight exceeding the 40-byte maximum. Caffeine 3's W-TinyLFU
    // admission policy chooses which to keep based on frequency; we don't assert on identity,
    // only that eviction happened and the cache shrank back under its bound.
    cache.put(key1, val1);
    cache.put(key2, val2);
    cache.getCache().cleanUp();

    Assertions.assertTrue(
        cache.getCache().stats().evictionWeight() > 0,
        "Expected eviction weight > 0 after exceeding max size, got "
        + cache.getCache().stats().evictionWeight()
    );
    Assertions.assertEquals(1, cache.getCache().asMap().size());
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
    Assertions.assertEquals(0L, stats.getNumEntries());
    Assertions.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assertions.assertEquals(1L, stats.getNumEntries());
    Assertions.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assertions.assertEquals(1L, stats.getNumEntries());
    Assertions.assertEquals(34L, stats.getSizeInBytes());
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
    Assertions.assertEquals(0L, stats.getNumEntries());
    Assertions.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assertions.assertEquals(1L, stats.getNumEntries());
    Assertions.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assertions.assertEquals(2L, stats.getNumEntries());
    Assertions.assertEquals(68L, stats.getSizeInBytes());

    cache.close(namespace);
    stats = cache.getStats();
    Assertions.assertEquals(0, stats.getNumEntries());
    Assertions.assertEquals(0, stats.getSizeInBytes());
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
    Assertions.assertEquals(0L, stats.getNumEntries());
    Assertions.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assertions.assertEquals(1L, stats.getNumEntries());
    Assertions.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assertions.assertEquals(2L, stats.getNumEntries());
    Assertions.assertEquals(68L, stats.getSizeInBytes());
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
    Assertions.assertEquals(0L, stats.getNumEntries());
    Assertions.assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    Assertions.assertEquals(1L, stats.getNumEntries());
    Assertions.assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    Assertions.assertEquals(2L, stats.getNumEntries());
    Assertions.assertEquals(68L, stats.getSizeInBytes());
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
        ImmutableList.of(
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
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get();
    Assertions.assertEquals(10, config.getExpireAfter());
    Assertions.assertEquals(100, config.getSizeInBytes());
    Assertions.assertNotNull(config.createExecutor());
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
        ImmutableList.of(
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
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get();
    Assertions.assertEquals(10, config.getExpireAfter());
    Assertions.assertEquals(100, config.getSizeInBytes());
    Assertions.assertEquals(ForkJoinPool.commonPool(), config.createExecutor());
  }

  @Test
  public void testDefaultFromProperties()
  {
    final String keyPrefix = "cache.config.prefix";
    final Properties properties = new Properties();
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
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
    final CaffeineCacheConfig config = caffeineCacheConfigJsonConfigProvider.get();
    Assertions.assertEquals(-1, config.getExpireAfter());
    Assertions.assertEquals(-1L, config.getSizeInBytes());
    Assertions.assertEquals(ForkJoinPool.commonPool(), config.createExecutor());
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
