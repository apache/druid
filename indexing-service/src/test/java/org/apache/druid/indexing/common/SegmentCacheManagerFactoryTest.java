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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import com.google.inject.util.Providers;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLoadingThreadPool;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

class SegmentCacheManagerFactoryTest
{
  @TempDir
  File tempDir;

  private ObjectMapper jsonMapper;

  @BeforeAll
  static void setUpClass()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @BeforeEach
  void setUp()
  {
    jsonMapper = TestHelper.makeJsonMapper();
  }

  @Test
  void testVirtualStorageManagersShareTheInjectedPool()
  {
    final StorageLoadingThreadPool shared =
        StorageLoadingThreadPool.createFromConfig(new SegmentLoaderConfig().withVirtualStorage(true));
    try {
      final SegmentCacheManagerFactory factory =
          new SegmentCacheManagerFactory(TestIndex.INDEX_IO, jsonMapper, Providers.of(shared));
      final SegmentCacheManager m1 = factory.manufacturate(new File(tempDir, "a"), null, true, false);
      final SegmentCacheManager m2 = factory.manufacturate(new File(tempDir, "b"), null, true, false);

      Assertions.assertSame(shared, m1.getLoadingThreadPool());
      Assertions.assertSame(shared, m2.getLoadingThreadPool());
    }
    finally {
      shared.stop();
    }
  }

  @Test
  void testNonVirtualStorageDoesNotResolveTheLoadingPool()
  {
    // Injecting the factory into a virtualStorage=false-only consumer (e.g. DruidInputSource) must not force the
    // ephemeral loading pool to be created. The provider is only resolved on the virtualStorage=true path.
    final Provider<StorageLoadingThreadPool> throwingProvider = () -> {
      throw new AssertionError("ephemeral loading pool must not be resolved for virtualStorage=false");
    };
    final SegmentCacheManagerFactory factory =
        new SegmentCacheManagerFactory(TestIndex.INDEX_IO, jsonMapper, throwingProvider);

    final SegmentCacheManager m = factory.manufacturate(new File(tempDir, "c"), null, false, false);
    Assertions.assertFalse(m.getLoadingThreadPool().isAvailable());
  }

  @Test
  void testCreateWithOwnedPoolBuildsOneAvailablePoolPerFactory()
  {
    final SegmentCacheManagerFactory factory =
        SegmentCacheManagerFactory.createWithOwnedPool(TestIndex.INDEX_IO, jsonMapper);
    final SegmentCacheManager m1 = factory.manufacturate(new File(tempDir, "d"), null, true, false);
    final SegmentCacheManager m2 = factory.manufacturate(new File(tempDir, "e"), null, true, false);
    try {
      Assertions.assertTrue(m1.getLoadingThreadPool().isAvailable());
      // createWithOwnedPool builds one pool and shares it across the factory's manufacturate calls.
      Assertions.assertSame(m1.getLoadingThreadPool(), m2.getLoadingThreadPool());
    }
    finally {
      m1.getLoadingThreadPool().stop();
    }
  }
}
