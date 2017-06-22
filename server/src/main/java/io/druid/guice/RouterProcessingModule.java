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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.client.cache.CacheConfig;
import io.druid.collections.BlockingPool;
import io.druid.collections.DummyBlockingPool;
import io.druid.collections.DummyNonBlockingPool;
import io.druid.collections.NonBlockingPool;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Merging;
import io.druid.guice.annotations.Processing;
import io.druid.java.util.common.concurrent.ExecutorServiceConfig;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.ExecutorServiceMonitor;
import io.druid.server.metrics.MetricsModule;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

/**
 * This module is used to fulfill dependency injection of query processing and caching resources: buffer pools and
 * thread pools on Router Druid node type. Router needs to inject those resources, because it depends on
 * {@link io.druid.query.QueryToolChest}s, and they couple query type aspects not related to processing and caching,
 * which Router uses, and related to processing and caching, which Router doesn't use, but they inject the resources.
 */
public class RouterProcessingModule implements Module
{
  private static final Logger log = new Logger(RouterProcessingModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bind(ExecutorServiceConfig.class).to(DruidProcessingConfig.class);
    MetricsModule.register(binder, ExecutorServiceMonitor.class);
  }

  @Provides
  @BackgroundCaching
  @LazySingleton
  public ExecutorService getBackgroundExecutorService(CacheConfig cacheConfig)
  {
    if (cacheConfig.getNumBackgroundThreads() > 0) {
      log.error(
          "numBackgroundThreads[%d] configured, that is ignored on Router",
          cacheConfig.getNumBackgroundThreads()
      );
    }
    return Execs.dummy();
  }

  @Provides
  @Processing
  @ManageLifecycle
  public ExecutorService getProcessingExecutorService(DruidProcessingConfig config)
  {
    if (config.getNumThreadsConfigured() != ExecutorServiceConfig.DEFAULT_NUM_THREADS) {
      log.error("numThreads[%d] configured, that is ignored on Router", config.getNumThreadsConfigured());
    }
    return Execs.dummy();
  }

  @Provides
  @LazySingleton
  @Global
  public NonBlockingPool<ByteBuffer> getIntermediateResultsPool()
  {
    return DummyNonBlockingPool.instance();
  }

  @Provides
  @LazySingleton
  @Merging
  public BlockingPool<ByteBuffer> getMergeBufferPool(DruidProcessingConfig config)
  {
    if (config.getNumMergeBuffersConfigured() != DruidProcessingConfig.DEFAULT_NUM_MERGE_BUFFERS) {
      log.error(
          "numMergeBuffers[%d] configured, that is ignored on Router",
          config.getNumMergeBuffersConfigured()
      );
    }
    return DummyBlockingPool.instance();
  }
}
