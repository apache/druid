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

package io.druid.server.lookup.namespace.cache;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.concurrent.Execs;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.server.DruidNode;
import io.druid.server.lookup.namespace.NamespaceExtractionModule;
import io.druid.server.metrics.NoopServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class OffHeapNamespaceExtractionCacheManagerTest
{
  @Test
  public void testInjection()
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null)
                );
              }
            }
        )
    );
    final Properties properties = injector.getInstance(Properties.class);
    properties.clear();
    properties.put(NamespaceExtractionModule.TYPE_PREFIX, "offHeap");
    final NamespaceExtractionCacheManager manager = injector.getInstance(NamespaceExtractionCacheManager.class);
    Assert.assertEquals(OffHeapNamespaceExtractionCacheManager.class, manager.getClass());
  }

  @Test(timeout = 30000L)
  public void testRacyCreation() throws Exception
  {
    final int concurrentThreads = 100;
    final Lifecycle lifecycle = new Lifecycle();
    final ServiceEmitter emitter = new NoopServiceEmitter();
    final OffHeapNamespaceExtractionCacheManager manager = new OffHeapNamespaceExtractionCacheManager(
        lifecycle,
        emitter,
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>>of()
    );
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(Execs.multiThreaded(
        concurrentThreads,
        "offheaptest-%s"
    ));
    final List<ListenableFuture<?>> futures = new ArrayList<>();
    final CountDownLatch thunder = new CountDownLatch(1);
    final List<String> namespaceIds = new ArrayList<>();
    for (int i = 0; i < 5; ++i) {
      final String namespace = "namespace-" + UUID.randomUUID().toString();
      final String cacheKey = "initial-cache-" + namespace;
      namespaceIds.add(namespace);
      manager.getCacheMap(cacheKey).put("foo", "bar");
      Assert.assertFalse(manager.swapAndClearCache(namespace, cacheKey));
    }
    final Random random = new Random(3748218904L);
    try {
      for (int i = 0; i < concurrentThreads; ++i) {
        final int j = i;
        final String namespace = namespaceIds.get(random.nextInt(namespaceIds.size()));
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
                  final String cacheKey = String.format("%s-%d-key-%d", namespace, j, i);
                  manager.getCacheMap(cacheKey).put("foo", "bar" + Integer.toString(i));
                  Assert.assertTrue(manager.swapAndClearCache(namespace, cacheKey));
                }
              }
            }
        ));
      }
      thunder.countDown();
      Futures.allAsList(futures).get();
    }
    finally {
      service.shutdownNow();
    }

    for (final String namespace : namespaceIds) {
      Assert.assertEquals(ImmutableMap.of("foo", "bar999"), manager.getCacheMap(namespace));
    }
  }
}
