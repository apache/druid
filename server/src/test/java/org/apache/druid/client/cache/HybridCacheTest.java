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
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.CacheModule;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class HybridCacheTest
{
  private static final byte[] HI = StringUtils.toUtf8("hi");

  @Test
  public void testInjection()
  {
    final String prefix = "testInjectHybridCache";
    System.setProperty(prefix + ".type", "hybrid");
    System.setProperty(prefix + ".l1.type", "local");
    System.setProperty(prefix + ".l2.type", "memcached");
    System.setProperty(prefix + ".useL2", "false");
    System.setProperty(prefix + ".l2.hosts", "localhost:11711");

    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("hybridTest");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
                binder.install(new CacheModule(prefix));
              }
            }
        )
    );
    final CacheProvider cacheProvider = injector.getInstance(Key.get(CacheProvider.class, Global.class));
    Assertions.assertNotNull(cacheProvider);
    Assertions.assertEquals(HybridCacheProvider.class, cacheProvider.getClass());

    final Cache cache = cacheProvider.get();
    Assertions.assertNotNull(cache);

    Assertions.assertFalse(cache.isLocal());
    Assertions.assertFalse(((HybridCacheProvider) cacheProvider).getUseL2());
    Assertions.assertTrue(((HybridCacheProvider) cacheProvider).getPopulateL2());
    Assertions.assertEquals(LocalCacheProvider.class, ((HybridCacheProvider) cacheProvider).level1.getClass());
    Assertions.assertEquals(MemcachedCacheProvider.class, ((HybridCacheProvider) cacheProvider).level2.getClass());
  }

  @Test
  public void testSanity() throws IOException
  {
    final ByteCountingLRUMap l1Map = new ByteCountingLRUMap(1024 * 1024);
    final ByteCountingLRUMap l2Map = new ByteCountingLRUMap(1024 * 1024);
    final MapCache l1 = new MapCache(l1Map);
    final MapCache l2 = new MapCache(l2Map);
    HybridCache cache = new HybridCache(new HybridCacheConfig(), l1, l2);

    final Cache.NamedKey key1 = new Cache.NamedKey("a", HI);
    final Cache.NamedKey key2 = new Cache.NamedKey("b", HI);
    final Cache.NamedKey key3 = new Cache.NamedKey("c", HI);
    final Cache.NamedKey key4 = new Cache.NamedKey("d", HI);

    final byte[] value1 = Ints.toByteArray(1);
    final byte[] value2 = Ints.toByteArray(2);
    final byte[] value3 = Ints.toByteArray(3);



    // test put puts to both
    cache.put(key1, value1);
    Assertions.assertEquals(value1, l1.get(key1));
    Assertions.assertEquals(value1, l2.get(key1));
    Assertions.assertEquals(value1, cache.get(key1));

    int hits = 0;
    Assertions.assertEquals(0, cache.getStats().getNumMisses());
    Assertions.assertEquals(++hits, cache.getStats().getNumHits());

    // test l1
    l1.put(key2, value2);
    Assertions.assertEquals(value2, cache.get(key2));
    Assertions.assertEquals(0, cache.getStats().getNumMisses());
    Assertions.assertEquals(++hits, cache.getStats().getNumHits());

    // test l2
    l2.put(key3, value3);
    Assertions.assertEquals(value3, cache.get(key3));
    Assertions.assertEquals(0, cache.getStats().getNumMisses());
    Assertions.assertEquals(++hits, cache.getStats().getNumHits());


    // test bulk get with l1 and l2
    {
      final HashSet<Cache.NamedKey> keys = Sets.newHashSet(key1, key2, key3);
      Map<Cache.NamedKey, byte[]> res = cache.getBulk(keys);
      Assertions.assertNotNull(res);
      Assertions.assertEquals(keys, res.keySet());
      Assertions.assertArrayEquals(value1, res.get(key1));
      Assertions.assertArrayEquals(value2, res.get(key2));
      Assertions.assertArrayEquals(value3, res.get(key3));

      hits += 3;
      Assertions.assertEquals(0, cache.getStats().getNumMisses());
      Assertions.assertEquals(hits, cache.getStats().getNumHits());
    }

    // test bulk get with l1 entries only
    {
      final HashSet<Cache.NamedKey> keys = Sets.newHashSet(key1, key2);
      Map<Cache.NamedKey, byte[]> res = cache.getBulk(keys);
      Assertions.assertNotNull(res);
      Assertions.assertEquals(keys, res.keySet());
      Assertions.assertArrayEquals(value1, res.get(key1));
      Assertions.assertArrayEquals(value2, res.get(key2));

      hits += 2;
      Assertions.assertEquals(0, cache.getStats().getNumMisses());
      Assertions.assertEquals(hits, cache.getStats().getNumHits());
    }

    int misses = 0;
    Assertions.assertNull(cache.get(key4));
    Assertions.assertEquals(++misses, cache.getStats().getNumMisses());

    Assertions.assertTrue(cache.getBulk(Sets.newHashSet(key4)).isEmpty());
    Assertions.assertEquals(++misses, cache.getStats().getNumMisses());

    {
      final Map<Cache.NamedKey, byte[]> res = cache.getBulk(Sets.newHashSet(key1, key4));
      Assertions.assertEquals(Sets.newHashSet(key1), res.keySet());
      Assertions.assertArrayEquals(value1, res.get(key1));

      Assertions.assertEquals(++hits, cache.getStats().getNumHits());
      Assertions.assertEquals(++misses, cache.getStats().getNumMisses());
    }

    {
      final Map<Cache.NamedKey, byte[]> res = cache.getBulk(Sets.newHashSet(key3, key4));
      Assertions.assertEquals(Sets.newHashSet(key3), res.keySet());
      Assertions.assertArrayEquals(value3, res.get(key3));

      Assertions.assertEquals(hits + 1, cache.getStats().getNumHits());
      Assertions.assertEquals(misses + 1, cache.getStats().getNumMisses());
    }

    // test close
    cache.close();
    Assertions.assertEquals(0, l1Map.size(), "l1 size after close()");
    Assertions.assertEquals(0, l2Map.size(), "l2 size after close()");
  }
}
