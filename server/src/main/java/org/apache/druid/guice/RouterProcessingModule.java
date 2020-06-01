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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DummyBlockingPool;
import org.apache.druid.collections.DummyNonBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ExecutorServiceConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ExecutorServiceMonitor;
import org.apache.druid.server.metrics.MetricsModule;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

/**
 * This module is used to fulfill dependency injection of query processing and caching resources: buffer pools and
 * thread pools on Router Druid node type. Router needs to inject those resources, because it depends on
 * {@link org.apache.druid.query.QueryToolChest}s, and they couple query type aspects not related to processing and
 * caching, which Router uses, and related to processing and caching, which Router doesn't use, but they inject the
 * resources.
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
