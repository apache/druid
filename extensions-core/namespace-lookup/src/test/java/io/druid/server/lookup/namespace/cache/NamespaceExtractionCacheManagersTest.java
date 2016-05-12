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

package io.druid.server.lookup.namespace.cache;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.server.metrics.NoopServiceEmitter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class NamespaceExtractionCacheManagersTest
{
  private static final Logger log = new Logger(NamespaceExtractionCacheManagersTest.class);
  private static final Lifecycle lifecycle = new Lifecycle();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> getParameters()
  {
    ArrayList<Object[]> params = new ArrayList<>();
    params.add(
        new Object[]{
            new OffHeapNamespaceExtractionCacheManager(
                lifecycle,
                new NoopServiceEmitter(),
                ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>>of()
            )
        }
    );
    params.add(
        new Object[]{
            new OnHeapNamespaceExtractionCacheManager(
                lifecycle,
                new NoopServiceEmitter(),
                ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>>of()
            )
        }
    );
    return params;
  }

  private final NamespaceExtractionCacheManager extractionCacheManager;

  public NamespaceExtractionCacheManagersTest(
      NamespaceExtractionCacheManager extractionCacheManager
  )
  {
    this.extractionCacheManager = extractionCacheManager;
  }

  private static final List<String> nsList = ImmutableList.<String>of("testNs", "test.ns", "//tes-tn!s");

  @Before
  public void setup()
  {
    // prepopulate caches
    for (String ns : nsList) {
      final ConcurrentMap<String, String> map = extractionCacheManager.getCacheMap(ns);
      map.put("oldNameSeed1", "oldNameSeed2");
    }
  }

  @Test
  public void testSimpleCacheCreate()
  {
    for (String ns : nsList) {
      ConcurrentMap<String, String> map = extractionCacheManager.getCacheMap(ns);
      map.put("key", "val");
      Assert.assertEquals("val", map.get("key"));
      Assert.assertEquals("val", extractionCacheManager.getCacheMap(ns).get("key"));
    }
  }

  @Test
  public void testSimpleCacheSwap()
  {
    for (String ns : nsList) {
      ConcurrentMap<String, String> map = extractionCacheManager.getCacheMap(ns + "old_cache");
      map.put("key", "val");
      extractionCacheManager.swapAndClearCache(ns, ns + "old_cache");
      Assert.assertEquals("val", map.get("key"));
      Assert.assertEquals("val", extractionCacheManager.getCacheMap(ns).get("key"));

      ConcurrentMap<String, String> map2 = extractionCacheManager.getCacheMap(ns + "cache");
      map2.put("key", "val2");
      Assert.assertTrue(extractionCacheManager.swapAndClearCache(ns, ns + "cache"));
      Assert.assertEquals("val2", map2.get("key"));
      Assert.assertEquals("val2", extractionCacheManager.getCacheMap(ns).get("key"));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingCacheThrowsIAE()
  {
    for (String ns : nsList) {
      ConcurrentMap<String, String> map = extractionCacheManager.getCacheMap(ns);
      map.put("key", "val");
      Assert.assertEquals("val", map.get("key"));
      Assert.assertEquals("val", extractionCacheManager.getCacheMap(ns).get("key"));
      Assert.assertFalse(extractionCacheManager.swapAndClearCache(ns, "I don't exist"));
    }
  }

  @Test
  public void testCacheList()
  {
    List<String> nsList = new ArrayList<String>(NamespaceExtractionCacheManagersTest.nsList);
    for (String ns : nsList) {
      extractionCacheManager.implData.put(ns, new NamespaceExtractionCacheManager.NamespaceImplData(null, null, null));
    }
    List<String> retvalList = Lists.newArrayList(extractionCacheManager.getKnownIDs());
    Collections.sort(nsList);
    Collections.sort(retvalList);
    Assert.assertArrayEquals(nsList.toArray(), retvalList.toArray());
  }

  @Test
  public void testNoDeleteNonexistant()
  {
    Assert.assertFalse(extractionCacheManager.delete("I don't exist"));
  }

  public static void waitFor(Future<?> future) throws InterruptedException
  {
    while (!future.isDone()) {
      try {
        future.get();
      }
      catch (ExecutionException e) {
        log.error(e.getCause(), "Error waiting");
        throw Throwables.propagate(e.getCause());
      }
    }
  }
}
