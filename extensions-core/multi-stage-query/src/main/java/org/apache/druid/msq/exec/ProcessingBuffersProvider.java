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

package org.apache.druid.msq.exec;

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.msq.kernel.QueryDefinition;

/**
 * Provides processing buffers for {@link org.apache.druid.msq.kernel.WorkOrder}. Thread-safe, shared by all
 * {@link Worker} in a particular JVM.
 */
public interface ProcessingBuffersProvider
{
  /**
   * Acquire buffers for a {@link Worker}.
   */
  ResourceHolder<ProcessingBuffersSet> acquire(int poolSize, long timeoutMillis);

  /**
   * Acquire buffers for a {@link Worker}, using a pool size equal to the minimum of
   * {@link WorkerContext#maxConcurrentStages()} and the number of stages in the query where
   * {@link StageProcessor#usesProcessingBuffers()}. (These are both caps on the number of concurrent
   * stages that will need processing buffers at once.)
   */
  default ResourceHolder<ProcessingBuffersSet> acquire(
      final QueryDefinition queryDef,
      final int maxConcurrentStages
  )
  {
    final int poolSize = Math.min(
        maxConcurrentStages,
        (int) queryDef.getStageDefinitions()
                      .stream()
                      .filter(stageDef -> stageDef.getProcessor().usesProcessingBuffers())
                      .count()
    );

    return acquire(poolSize, queryDef.getContext().getTimeout());
  }
}
