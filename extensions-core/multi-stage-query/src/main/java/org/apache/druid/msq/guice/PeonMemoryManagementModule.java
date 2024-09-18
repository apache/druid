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

package org.apache.druid.msq.guice;

import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.MemoryIntrospectorImpl;
import org.apache.druid.msq.exec.ProcessingBuffersProvider;
import org.apache.druid.msq.indexing.PeonProcessingBuffersProvider;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.utils.JvmUtils;

import java.nio.ByteBuffer;

/**
 * Provides {@link MemoryIntrospector} for single-task-per-JVM model.
 *
 * @see IndexerMemoryManagementModule for multi-task-per-JVM model used on {@link org.apache.druid.cli.CliIndexer}
 */
@LoadScope(roles = NodeRole.PEON_JSON_NAME)

public class PeonMemoryManagementModule implements DruidModule
{
  /**
   * Peons have a single worker per JVM.
   */
  private static final int NUM_WORKERS_IN_JVM = 1;

  /**
   * Allocate 75% of memory for the MSQ framework.
   */
  private static final double USABLE_MEMORY_FRACTION = 0.75;

  @Override
  public void configure(Binder binder)
  {
    TaskMemoryManagementConfig.bind(binder);
  }

  @Provides
  @LazySingleton
  public MemoryIntrospector createMemoryIntrospector(
      final LookupExtractorFactoryContainerProvider lookupProvider,
      final DruidProcessingConfig processingConfig,
      final TaskMemoryManagementConfig taskMemoryManagementConfig
  )
  {
    return new MemoryIntrospectorImpl(
        JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(),
        USABLE_MEMORY_FRACTION,
        NUM_WORKERS_IN_JVM,
        getNumThreads(taskMemoryManagementConfig, processingConfig),
        lookupProvider
    );
  }

  @Provides
  @LazySingleton
  public ProcessingBuffersProvider createProcessingBuffersProvider(
      @Global final NonBlockingPool<ByteBuffer> processingPool,
      final MemoryIntrospector memoryIntrospector
  )
  {
    return new PeonProcessingBuffersProvider(
        processingPool,
        memoryIntrospector.numProcessingThreads()
    );
  }

  public static int getNumThreads(
      final TaskMemoryManagementConfig taskMemoryManagementConfig,
      final DruidProcessingConfig processingConfig
  )
  {
    if (taskMemoryManagementConfig.getMaxThreads() == TaskMemoryManagementConfig.UNLIMITED) {
      return processingConfig.getNumThreads();
    } else if (taskMemoryManagementConfig.getMaxThreads() > 0) {
      return Math.min(taskMemoryManagementConfig.getMaxThreads(), processingConfig.getNumThreads());
    } else {
      throw new IAE(
          "Invalid value of %s.maxThreads[%d]",
          TaskMemoryManagementConfig.BASE_PROPERTY,
          taskMemoryManagementConfig.getMaxThreads()
      );
    }
  }
}
