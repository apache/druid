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

package org.apache.druid.msq.dart.guice;

import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.worker.DartProcessingBuffersProvider;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.MemoryIntrospectorImpl;
import org.apache.druid.msq.exec.ProcessingBuffersProvider;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.utils.JvmUtils;

import java.nio.ByteBuffer;

/**
 * Memory management module for Historicals.
 */
@LoadScope(roles = {NodeRole.HISTORICAL_JSON_NAME})
public class DartWorkerMemoryManagementModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    // Nothing to do.
  }

  @Provides
  public MemoryIntrospector createMemoryIntrospector(
      final DartWorkerConfig workerConfig,
      final DruidProcessingConfig druidProcessingConfig
  )
  {
    return new MemoryIntrospectorImpl(
        JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(),
        workerConfig.getHeapFraction(),
        computeConcurrentQueries(workerConfig, druidProcessingConfig),
        druidProcessingConfig.getNumThreads(),
        null
    );
  }

  @Provides
  @Dart
  @LazySingleton
  public ProcessingBuffersProvider createProcessingBuffersProvider(
      @Merging final BlockingPool<ByteBuffer> mergeBufferPool,
      final DruidProcessingConfig processingConfig
  )
  {
    return new DartProcessingBuffersProvider(mergeBufferPool, processingConfig.getNumThreads());
  }

  private static int computeConcurrentQueries(
      final DartWorkerConfig workerConfig,
      final DruidProcessingConfig processingConfig
  )
  {
    if (workerConfig.getConcurrentQueries() == DartWorkerConfig.AUTO) {
      return processingConfig.getNumMergeBuffers();
    } else if (workerConfig.getConcurrentQueries() < 0) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("concurrentQueries[%s] must be positive or -1", workerConfig.getConcurrentQueries());
    } else if (workerConfig.getConcurrentQueries() > processingConfig.getNumMergeBuffers()) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(
                              "concurrentQueries[%s] must be less than numMergeBuffers[%s]",
                              workerConfig.getConcurrentQueries(),
                              processingConfig.getNumMergeBuffers()
                          );
    } else {
      return workerConfig.getConcurrentQueries();
    }
  }
}
