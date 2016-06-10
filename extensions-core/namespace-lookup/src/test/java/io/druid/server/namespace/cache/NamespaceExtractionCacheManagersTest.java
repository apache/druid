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

package io.druid.server.namespace.cache;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.server.metrics.NoopServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
@RunWith(Parameterized.class)
public class NamespaceExtractionCacheManagersTest
{
  private static final Logger log = new Logger(NamespaceExtractionCacheManagersTest.class);
  private static final Lifecycle lifecycle = new Lifecycle();

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    ArrayList<Object[]> params = new ArrayList<>();

    ConcurrentMap<String, Function<String, String>> fnMap = new ConcurrentHashMap<String, Function<String, String>>();
    ConcurrentMap<String, Function<String, List<String>>> reverserFnMap = new ConcurrentHashMap<String, Function<String, List<String>>>();
    params.add(
        new Object[]{
                   new OffHeapNamespaceExtractionCacheManager(
                   lifecycle,
                   fnMap,
                   reverserFnMap,
                   new NoopServiceEmitter(),
                   ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of()
               ), fnMap
               }
    );
    params.add(
        new Object[]{
            new OnHeapNamespaceExtractionCacheManager(
                lifecycle,
                fnMap,
                reverserFnMap,
                new NoopServiceEmitter(),
                ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of()
            ), fnMap
        }
    );
    return params;
  }

  private final NamespaceExtractionCacheManager extractionCacheManager;
  private final ConcurrentMap<String, Function<String, String>> fnMap;

  public NamespaceExtractionCacheManagersTest(
      NamespaceExtractionCacheManager extractionCacheManager,
      ConcurrentMap<String, Function<String, String>> fnMap
  )
  {
    this.extractionCacheManager = extractionCacheManager;
    this.fnMap = fnMap;
  }

  private static final List<String> nsList = ImmutableList.<String>of("testNs", "test.ns", "//tes-tn!s");

  @Before
  public void setup()
  {
    fnMap.clear();
    // prepopulate caches
    for (String ns : nsList) {
      final ConcurrentMap<String, String> map = extractionCacheManager.getCacheMap(ns);
      fnMap.put(
          ns, new Function<String, String>()
          {
            @Nullable
            @Override
            public String apply(String input)
            {
              return map.get(input);
            }
          }
      );
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
    List<String> nsList = new ArrayList<String>(NamespaceExtractionCacheManagersTest.nsList);
    List<String> retvalList = Lists.newArrayList(extractionCacheManager.getKnownNamespaces());
    Collections.sort(nsList);
    Collections.sort(retvalList);
    Assert.assertArrayEquals(nsList.toArray(), retvalList.toArray());
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
