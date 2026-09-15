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

package org.apache.druid.server.lookup.namespace.cache;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NamespaceExtractionCacheManagersTest
{
  public static Collection<Object[]> data()
  {
    return Arrays.asList(new Object[][]{
        {CacheSchedulerTest.CREATE_ON_HEAP_CACHE_MANAGER},
        {CacheSchedulerTest.CREATE_OFF_HEAP_CACHE_MANAGER}
    });
  }

  private Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager manager;

  public void initNamespaceExtractionCacheManagersTest(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager)
  {

    this.createCacheManager = createCacheManager;
    manager = createCacheManager.apply(lifecycle);
  }

  @BeforeEach
  public void setUp() throws Exception
  {
    lifecycle = new Lifecycle();
    lifecycle.start();
  }

  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testRacyCreation(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws Exception
  {
    initNamespaceExtractionCacheManagersTest(createCacheManager);
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
            () -> {
              try {
                thunder.await();
              }
              catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              for (int i1 = 0; i1 < 1000; ++i1) {
                CacheHandler cacheHandler = manager.createCache();
                cacheHandler.close();
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

    Assertions.assertEquals(0, manager.cacheCount());
  }

  /**
   * Tests that even if CacheHandler.close() wasn't called, the cache is cleaned up when it becomes unreachable.
   */
  @MethodSource("data")
  @ParameterizedTest
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testCacheCloseForgotten(Function<Lifecycle, NamespaceExtractionCacheManager> createCacheManager) throws InterruptedException
  {
    initNamespaceExtractionCacheManagersTest(createCacheManager);
    Assertions.assertEquals(0, manager.cacheCount());
    createDanglingCache();
    Assertions.assertEquals(1, manager.cacheCount());
    while (manager.cacheCount() > 0) {
      System.gc();
      Thread.sleep(1000);
    }
    Assertions.assertEquals(0, manager.cacheCount());
  }

  private void createDanglingCache()
  {
    manager.createCache();
  }
}
