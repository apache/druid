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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class NamespaceExtractionCacheManagersTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> data()
  {
    return Arrays.asList(new Object[][]{
        {CacheSchedulerTest.CREATE_ON_HEAP_CACHE_MANAGER},
        {CacheSchedulerTest.CREATE_OFF_HEAP_CACHE_MANAGER}
    });
  }

  private final Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager manager;

  public NamespaceExtractionCacheManagersTest(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager)
  {

    this.createCacheManager = createCacheManager;
  }

  @Before
  public void setUp() throws Exception
  {
    lifecycle = new Lifecycle();
    lifecycle.start();
    manager = createCacheManager.apply(lifecycle);
  }

  @Test(timeout = 30000L)
  public void testRacyCreation() throws Exception
  {
    final int concurrentThreads = 10;
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(Execs.multiThreaded(
        concurrentThreads,
        "offheaptest-%s"
    ));
    final List<ListenableFuture<?>> futures = new ArrayList<>();
    final CountDownLatch thunder = new CountDownLatch(1);
    try {
      for (int i = 0; i < concurrentThreads; ++i) {
        futures.add(service.submit(
            new Runnable()
            {
              @Override
              public void run()
              {
                try {
                  thunder.await();
                }
                catch (InterruptedException e) {
                  throw Throwables.propagate(e);
                }
                for (int i = 0; i < 1000; ++i) {
                  CacheHandler cacheHandler = manager.createCache();
                  cacheHandler.close();
                }
              }
            }
        ));
      }
      thunder.countDown();
      Futures.allAsList(futures).get();
    }
    finally {
      service.shutdown();
      service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    Assert.assertEquals(0, manager.cacheCount());
  }

  /**
   * Tests that even if CacheHandler.close() wasn't called, the cache is cleaned up when it becomes unreachable.
   */
  @Test(timeout = 60_000)
  public void testCacheCloseForgotten() throws InterruptedException
  {
    Assert.assertEquals(0, manager.cacheCount());
    createDanglingCache();
    Assert.assertEquals(1, manager.cacheCount());
    while (manager.cacheCount() > 0) {
      System.gc();
      Thread.sleep(1000);
    }
    Assert.assertEquals(0, manager.cacheCount());
  }

  private void createDanglingCache()
  {
    manager.createCache();
  }
}
