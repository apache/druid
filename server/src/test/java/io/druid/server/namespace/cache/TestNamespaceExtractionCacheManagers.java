/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.namespace.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.client.cache.LocalCacheProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
@RunWith(Parameterized.class)
public class TestNamespaceExtractionCacheManagers
{
  private static final Lifecycle lifecycle = new Lifecycle();

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
        new Object[]{new OffHeapNamespaceExtractionCacheManager(lifecycle)},
        new Object[]{new OnHeapNamespaceExtractionCacheManager(lifecycle)},
        new Object[]{new ClientCacheExtractionCacheManager(new LocalCacheProvider(500_000l, 500_000, 0).get(), lifecycle)}
    );
  }

  private final NamespaceExtractionCacheManager extractionCacheManager;

  public TestNamespaceExtractionCacheManagers(NamespaceExtractionCacheManager extractionCacheManager)
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
  public void testCacheList()
  {
    List<String> nsList = new ArrayList<String>(TestNamespaceExtractionCacheManagers.nsList);
    List<String> retvalList = Lists.newArrayList(extractionCacheManager.getKnownNamespaces());
    Collections.sort(nsList);
    Collections.sort(retvalList);
    Assert.assertArrayEquals(nsList.toArray(), retvalList.toArray());
  }
}
