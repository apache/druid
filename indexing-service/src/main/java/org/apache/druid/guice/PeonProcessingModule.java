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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DummyBlockingPool;
import org.apache.druid.collections.DummyNonBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ExecutorServiceMonitor;
import org.apache.druid.query.NoopQueryProcessingPool;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;

import java.nio.ByteBuffer;

/**
 * This module fulfills the dependency injection of query processing and caching resources: buffer pools and
 * thread pools on Peon selectively. Only the peons for the tasks supporting queries need to allocate direct buffers
 * and thread pools. Thus, this is separate from the {@link DruidProcessingModule} to separate the needs of the peons and
 * the historicals
 *
 * @see DruidProcessingModule
 */
public class PeonProcessingModule implements Module
{
  private static final Logger log = new Logger(PeonProcessingModule.class);

  @Override
  public void configure(Binder binder)
  {
    DruidProcessingModule.registerConfigsAndMonitor(binder);
  }

  @Provides
  @LazySingleton
  public CachePopulator getCachePopulator(
      @Smile ObjectMapper smileMapper,
      CachePopulatorStats cachePopulatorStats,
      CacheConfig cacheConfig
  )
  {
    return DruidProcessingModule.createCachePopulator(smileMapper, cachePopulatorStats, cacheConfig);
  }

  @Provides
  @ManageLifecycle
  public QueryProcessingPool getProcessingExecutorPool(
      Task task,
      DruidProcessingConfig config,
      ExecutorServiceMonitor executorServiceMonitor,
      Lifecycle lifecycle
  )
  {
    if (task.supportsQueries()) {
      return DruidProcessingModule.createProcessingExecutorPool(config, executorServiceMonitor, lifecycle);
    } else {
      if (config.isNumThreadsConfigured()) {
        log.warn(
            "Ignoring the configured numThreads[%d] because task[%s] of type[%s] does not support queries",
            config.getNumThreads(),
            task.getId(),
            task.getType()
        );
      }
      return NoopQueryProcessingPool.instance();
    }
  }

  @Provides
  @LazySingleton
  @Global
  public NonBlockingPool<ByteBuffer> getIntermediateResultsPool(Task task, DruidProcessingConfig config)
  {
    if (task.supportsQueries()) {
      return DruidProcessingModule.createIntermediateResultsPool(config);
    } else {
      return DummyNonBlockingPool.instance();
    }
  }

  @Provides
  @LazySingleton
  @Merging
  public BlockingPool<ByteBuffer> getMergeBufferPool(Task task, DruidProcessingConfig config)
  {
    if (task.supportsQueries()) {
      return DruidProcessingModule.createMergeBufferPool(config);
    } else {
      if (config.isNumMergeBuffersConfigured()) {
        log.warn(
            "Ignoring the configured numMergeBuffers[%d] because task[%s] of type[%s] does not support queries",
            config.getNumThreads(),
            task.getId(),
            task.getType()
        );
      }
      return DummyBlockingPool.instance();
    }
  }

  @Provides
  @LazySingleton
  @Merging
  public GroupByResourcesReservationPool getGroupByResourcesReservationPool(
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      GroupByQueryConfig groupByQueryConfig
  )
  {
    return new GroupByResourcesReservationPool(mergeBufferPool, groupByQueryConfig);
  }
}
