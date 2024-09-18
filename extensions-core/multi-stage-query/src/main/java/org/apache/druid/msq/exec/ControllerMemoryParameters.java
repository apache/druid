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

import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;

/**
 * Class for determining how much JVM heap to allocate to various purposes for {@link Controller}.
 *
 * First, look at how much of total JVM heap that is dedicated for MSQ; see
 * {@link MemoryIntrospector#memoryPerTask()}.
 *
 * Then, we split up that total amount of memory into equally-sized portions per {@link Controller}; see
 * {@link MemoryIntrospector#numTasksInJvm()}. The number of controllers is based entirely on server configuration,
 * which makes the calculation robust to different queries running simultaneously in the same JVM.
 *
 * Then, we split that up into a chunk used for input channels, and a chunk used for partition statistics.
 */
public class ControllerMemoryParameters
{
  /**
   * Maximum number of bytes that we'll ever use for maxRetainedBytes of {@link ClusterByStatisticsCollectorImpl}.
   */
  private static final long PARTITION_STATS_MAX_MEMORY = 300_000_000;

  /**
   * Minimum number of bytes that is allowable for maxRetainedBytes of {@link ClusterByStatisticsCollectorImpl}.
   */
  private static final long PARTITION_STATS_MIN_MEMORY = 25_000_000;

  /**
   * Memory allocated to {@link ClusterByStatisticsCollectorImpl} as part of {@link ControllerQueryKernel}.
   */
  private final int partitionStatisticsMaxRetainedBytes;

  public ControllerMemoryParameters(int partitionStatisticsMaxRetainedBytes)
  {
    this.partitionStatisticsMaxRetainedBytes = partitionStatisticsMaxRetainedBytes;
  }

  /**
   * Create an instance.
   *
   * @param memoryIntrospector memory introspector
   * @param maxWorkerCount     maximum worker count of the final stage
   */
  public static ControllerMemoryParameters createProductionInstance(
      final MemoryIntrospector memoryIntrospector,
      final int maxWorkerCount
  )
  {
    final long totalMemory = memoryIntrospector.memoryPerTask();
    final long memoryForInputChannels =
        WorkerMemoryParameters.computeProcessorMemoryForInputChannels(
            maxWorkerCount,
            WorkerMemoryParameters.DEFAULT_FRAME_SIZE
        );
    final int partitionStatisticsMaxRetainedBytes = (int) Math.min(
        totalMemory - memoryForInputChannels,
        PARTITION_STATS_MAX_MEMORY
    );

    if (partitionStatisticsMaxRetainedBytes < PARTITION_STATS_MIN_MEMORY) {
      final long requiredTaskMemory = memoryForInputChannels + PARTITION_STATS_MIN_MEMORY;
      throw new MSQException(
          new NotEnoughMemoryFault(
              memoryIntrospector.computeJvmMemoryRequiredForTaskMemory(requiredTaskMemory),
              memoryIntrospector.totalMemoryInJvm(),
              memoryIntrospector.memoryPerTask(),
              memoryIntrospector.numTasksInJvm(),
              memoryIntrospector.numProcessingThreads(),
              maxWorkerCount,
              1
          )
      );
    }

    return new ControllerMemoryParameters(partitionStatisticsMaxRetainedBytes);
  }

  /**
   * Memory allocated to {@link ClusterByStatisticsCollectorImpl} as part of {@link ControllerQueryKernel}.
   */
  public int getPartitionStatisticsMaxRetainedBytes()
  {
    return partitionStatisticsMaxRetainedBytes;
  }
}
