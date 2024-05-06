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
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.MemoryIntrospectorImpl;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.utils.JvmUtils;

/**
 * Provides {@link MemoryIntrospector} for multi-task-per-JVM model.
 *
 * @see PeonMemoryManagementModule for single-task-per-JVM model used on {@link org.apache.druid.cli.CliPeon}
 */
@LoadScope(roles = NodeRole.INDEXER_JSON_NAME)
public class IndexerMemoryManagementModule implements DruidModule
{
  /**
   * Allocate up to 75% of memory for MSQ-related stuff (if all running tasks are MSQ tasks).
   */
  private static final double USABLE_MEMORY_FRACTION = 0.75;

  @Override
  public void configure(Binder binder)
  {
    // Nothing to do.
  }

  @Provides
  @LazySingleton
  public Bouncer makeProcessorBouncer(final DruidProcessingConfig processingConfig)
  {
    return new Bouncer(processingConfig.getNumThreads());
  }

  @Provides
  @LazySingleton
  public MemoryIntrospector createMemoryIntrospector(
      final LookupExtractorFactoryContainerProvider lookupProvider,
      final DruidProcessingConfig processingConfig,
      final WorkerConfig workerConfig
  )
  {
    return new MemoryIntrospectorImpl(
        lookupProvider,
        JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(),
        USABLE_MEMORY_FRACTION,
        workerConfig.getCapacity(),
        processingConfig.getNumThreads()
    );
  }
}
