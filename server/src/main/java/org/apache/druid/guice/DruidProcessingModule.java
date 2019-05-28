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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import org.apache.druid.client.cache.BackgroundCachePopulator;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ExecutorServiceConfig;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ExecutorServiceMonitor;
import org.apache.druid.query.MetricsEmittingExecutorService;
import org.apache.druid.query.PrioritizedExecutorService;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.utils.JvmUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class DruidProcessingModule implements Module
{
  private static final Logger log = new Logger(DruidProcessingModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bind(ExecutorServiceConfig.class).to(DruidProcessingConfig.class);
    MetricsModule.register(binder, ExecutorServiceMonitor.class);
  }

  @Provides
  @LazySingleton
  public CachePopulator getCachePopulator(
      @Smile ObjectMapper smileMapper,
      CachePopulatorStats cachePopulatorStats,
      CacheConfig cacheConfig
  )
  {
    if (cacheConfig.getNumBackgroundThreads() > 0) {
      final ExecutorService exec = Executors.newFixedThreadPool(
          cacheConfig.getNumBackgroundThreads(),
          new ThreadFactoryBuilder()
              .setNameFormat("background-cacher-%d")
              .setDaemon(true)
              .setPriority(Thread.MIN_PRIORITY)
              .build()
      );

      return new BackgroundCachePopulator(exec, smileMapper, cachePopulatorStats, cacheConfig.getMaxEntrySize());
    } else {
      return new ForegroundCachePopulator(smileMapper, cachePopulatorStats, cacheConfig.getMaxEntrySize());
    }
  }

  @Provides
  @Processing
  @ManageLifecycle
  public ExecutorService getProcessingExecutorService(
      DruidProcessingConfig config,
      ExecutorServiceMonitor executorServiceMonitor,
      Lifecycle lifecycle
  )
  {
    return new MetricsEmittingExecutorService(
        PrioritizedExecutorService.create(
            lifecycle,
            config
        ),
        executorServiceMonitor
    );
  }

  @Provides
  @LazySingleton
  @Global
  public NonBlockingPool<ByteBuffer> getIntermediateResultsPool(DruidProcessingConfig config)
  {
    verifyDirectMemory(config);
    return new StupidPool<>(
        "intermediate processing pool",
        new OffheapBufferGenerator("intermediate processing", config.intermediateComputeSizeBytes()),
        config.getNumThreads(),
        config.poolCacheMaxCount()
    );
  }

  @Provides
  @LazySingleton
  @Merging
  public BlockingPool<ByteBuffer> getMergeBufferPool(DruidProcessingConfig config)
  {
    verifyDirectMemory(config);
    return new DefaultBlockingPool<>(
        new OffheapBufferGenerator("result merging", config.intermediateComputeSizeBytes()),
        config.getNumMergeBuffers()
    );
  }

  private void verifyDirectMemory(DruidProcessingConfig config)
  {
    try {
      final long maxDirectMemory = JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();
      final long memoryNeeded = (long) config.intermediateComputeSizeBytes() *
                                (config.getNumMergeBuffers() + config.getNumThreads() + 1);

      if (maxDirectMemory < memoryNeeded) {
        throw new ProvisionException(
            StringUtils.format(
                "Not enough direct memory.  Please adjust -XX:MaxDirectMemorySize, druid.processing.buffer.sizeBytes, druid.processing.numThreads, or druid.processing.numMergeBuffers: "
                + "maxDirectMemory[%,d], memoryNeeded[%,d] = druid.processing.buffer.sizeBytes[%,d] * (druid.processing.numMergeBuffers[%,d] + druid.processing.numThreads[%,d] + 1)",
                maxDirectMemory,
                memoryNeeded,
                config.intermediateComputeSizeBytes(),
                config.getNumMergeBuffers(),
                config.getNumThreads()
            )
        );
      }
    }
    catch (UnsupportedOperationException e) {
      log.debug("Checking for direct memory size is not support on this platform: %s", e);
      log.info(
          "Unable to determine max direct memory size. If druid.processing.buffer.sizeBytes is explicitly configured, "
          + "then make sure to set -XX:MaxDirectMemorySize to at least \"druid.processing.buffer.sizeBytes * "
          + "(druid.processing.numMergeBuffers[%,d] + druid.processing.numThreads[%,d] + 1)\", "
          + "or else set -XX:MaxDirectMemorySize to at least 25%% of maximum jvm heap size.",
          config.getNumMergeBuffers(),
          config.getNumThreads()
      );
    }
  }
}
