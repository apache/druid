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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class CaffeineCacheTest
{
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

  @BeforeEach
  void setUp()
  {
    cache = CaffeineCache.create(cacheConfig);
  }

  @Test
  void testBasicInjection() throws Exception
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
      assertEquals(CaffeineCache.class, cache.getClass());
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  void testSimpleInjection()
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
    assertNotNull(cacheProvider);
    assertEquals(CaffeineCacheProvider.class, cacheProvider.getClass());
  }

  @Test
  void testBaseOps()
  {
    final Cache.NamedKey aKey = new Cache.NamedKey("a", HI);
    assertNull(cache.get(aKey));
    put(cache, aKey, 1);
    assertEquals(1, get(cache, aKey));

    cache.close("a");
    assertNull(cache.get(aKey));

    final Cache.NamedKey hiKey = new Cache.NamedKey("the", HI);
    final Cache.NamedKey hoKey = new Cache.NamedKey("the", HO);
    put(cache, hiKey, 10);
    put(cache, hoKey, 20);
    assertEquals(10, get(cache, hiKey));
    assertEquals(20, get(cache, hoKey));
    cache.close("the");

    assertNull(cache.get(hiKey));
    assertNull(cache.get(hoKey));

    assertNull(cache.get(new Cache.NamedKey("miss", HI)));

    final CacheStats stats = cache.getStats();
    assertEquals(3, stats.getNumHits());
    assertEquals(5, stats.getNumMisses());
  }

  @Test
  void testGetBulk()
  {
    assertNull(cache.get(new Cache.NamedKey("the", HI)));

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

    assertEquals(2, Ints.fromByteArray(result.get(key1)));
    assertEquals(10, Ints.fromByteArray(result.get(key2)));

    Cache.NamedKey missingKey = new Cache.NamedKey("missing", HI);
    result = cache.getBulk(Collections.singletonList(missingKey));
    assertEquals(result.size(), 0);

    result = cache.getBulk(new ArrayList<>());
    assertEquals(result.size(), 0);
  }

  @Test
  void testSizeEviction()
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

    assertNull(cache.get(key1));
    assertNull(cache.get(key2));

    cache.put(key1, val1);
    assertArrayEquals(val1, cache.get(key1));
    assertNull(cache.get(key2));

    assertEquals(0, cache.getCache().stats().evictionWeight());

    assertArrayEquals(val1, cache.get(key1));
    assertNull(cache.get(key2));

    cache.put(key2, val2);
    assertNull(cache.get(key1));
    assertArrayEquals(val2, cache.get(key2));
    assertEquals(34, cache.getCache().stats().evictionWeight());
  }

  @Test
  void testSizeCalculation()
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
    assertEquals(0L, stats.getNumEntries());
    assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    assertEquals(1L, stats.getNumEntries());
    assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    assertEquals(1L, stats.getNumEntries());
    assertEquals(34L, stats.getSizeInBytes());
  }

  @Test
  void testSizeCalculationAfterDelete()
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
    assertEquals(0L, stats.getNumEntries());
    assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    assertEquals(1L, stats.getNumEntries());
    assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    assertEquals(2L, stats.getNumEntries());
    assertEquals(68L, stats.getSizeInBytes());

    cache.close(namespace);
    stats = cache.getStats();
    assertEquals(0, stats.getNumEntries());
    assertEquals(0, stats.getSizeInBytes());
  }


  @Test
  void testSizeCalculationMore()
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
    assertEquals(0L, stats.getNumEntries());
    assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    assertEquals(1L, stats.getNumEntries());
    assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    assertEquals(2L, stats.getNumEntries());
    assertEquals(68L, stats.getSizeInBytes());
  }

  @Test
  void testSizeCalculationNoWeight()
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
    assertEquals(0L, stats.getNumEntries());
    assertEquals(0L, stats.getSizeInBytes());

    cache.put(key1, val1);

    stats = cache.getStats();
    assertEquals(1L, stats.getNumEntries());
    assertEquals(34L, stats.getSizeInBytes());

    cache.put(key2, val2);

    stats = cache.getStats();
    assertEquals(2L, stats.getNumEntries());
    assertEquals(68L, stats.getSizeInBytes());
  }

  @Test
  void testFromProperties()
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
    assertEquals(10, config.getExpireAfter());
    assertEquals(100, config.getSizeInBytes());
    assertNotNull(config.createExecutor());
  }

  @Test
  void testMixedCaseFromProperties()
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
    assertEquals(10, config.getExpireAfter());
    assertEquals(100, config.getSizeInBytes());
    assertEquals(ForkJoinPool.commonPool(), config.createExecutor());
  }

  @Test
  void testDefaultFromProperties()
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
    assertEquals(-1, config.getExpireAfter());
    assertEquals(-1L, config.getSizeInBytes());
    assertEquals(ForkJoinPool.commonPool(), config.createExecutor());
  }

  public int get(Cache cache, Cache.NamedKey key)
  {
    return Ints.fromByteArray(Objects.requireNonNull(cache.get(key)));
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
