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
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ExecutorServiceConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ExecutorServiceMonitor;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.utils.JvmUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

/**
 * This module is used to fulfill dependency injection of query processing and caching resources: buffer pools and
 * thread pools on Broker. Broker does not need to be allocated an intermediate results pool.
 * This is separated from DruidProcessingModule to separate the needs of the broker from the historicals
 */

public class BrokerProcessingModule implements Module
{
  private static final Logger log = new Logger(BrokerProcessingModule.class);

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
  @ManageLifecycle
  public QueryProcessingPool getProcessingExecutorPool(
      DruidProcessingConfig config
  )
  {
    return new ForwardingQueryProcessingPool(Execs.dummy());
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
        config.getNumInitalBuffersForIntermediatePool(),
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

  @Provides
  @ManageLifecycle
  public LifecycleForkJoinPoolProvider getMergeProcessingPoolProvider(DruidProcessingConfig config)
  {
    return new LifecycleForkJoinPoolProvider(
        config.getMergePoolParallelism(),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        (t, e) -> log.error(e, "Unhandled exception in thread [%s]", t),
        true,
        config.getMergePoolAwaitShutdownMillis()
    );
  }

  @Provides
  @Merging
  public ForkJoinPool getMergeProcessingPool(LifecycleForkJoinPoolProvider poolProvider)
  {
    return poolProvider.getPool();
  }

  private void verifyDirectMemory(DruidProcessingConfig config)
  {
    final long memoryNeeded = (long) config.intermediateComputeSizeBytes() *
                              (config.getNumMergeBuffers() + 1);

    try {
      final long maxDirectMemory = JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();

      if (maxDirectMemory < memoryNeeded) {
        throw new ProvisionException(
            StringUtils.format(
                "Not enough direct memory.  Please adjust -XX:MaxDirectMemorySize, druid.processing.buffer.sizeBytes, or druid.processing.numMergeBuffers: "
                + "maxDirectMemory[%,d], memoryNeeded[%,d] = druid.processing.buffer.sizeBytes[%,d] * (druid.processing.numMergeBuffers[%,d] + 1)",
                maxDirectMemory,
                memoryNeeded,
                config.intermediateComputeSizeBytes(),
                config.getNumMergeBuffers()
            )
        );
      }
    }
    catch (UnsupportedOperationException e) {
      log.debug("Checking for direct memory size is not support on this platform: %s", e);
      log.info(
          "Your memory settings require at least %,d bytes of direct memory. "
          + "Your machine must have at least this much memory available, and your JVM "
          + "-XX:MaxDirectMemorySize parameter must be at least this high. "
          + "If it is, you may safely ignore this message. "
          + "Otherwise, consider adjusting your memory settings. "
          + "Calculation: druid.processing.buffer.sizeBytes[%,d] * (druid.processing.numMergeBuffers[%,d] + 1).",
          memoryNeeded,
          config.intermediateComputeSizeBytes(),
          config.getNumMergeBuffers()
      );
    }
  }
}
