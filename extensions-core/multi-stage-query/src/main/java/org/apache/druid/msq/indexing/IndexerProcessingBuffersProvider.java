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

package org.apache.druid.msq.indexing;

import org.apache.druid.cli.CliIndexer;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.ProcessingBuffersProvider;
import org.apache.druid.msq.exec.ProcessingBuffersSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Production implementation of {@link ProcessingBuffersProvider} for tasks in {@link CliIndexer}.
 */
public class IndexerProcessingBuffersProvider implements ProcessingBuffersProvider
{
  private static final int MIN_BUFFER_SIZE = 1_000_000;

  private final long heapMemoryToUse;
  private final int taskCapacity;
  private final int numThreads;

  public IndexerProcessingBuffersProvider(final long heapMemoryToUse, final int taskCapacity, final int numThreads)
  {
    this.heapMemoryToUse = heapMemoryToUse;
    this.taskCapacity = taskCapacity;
    this.numThreads = numThreads;
  }

  @Override
  public ResourceHolder<ProcessingBuffersSet> acquire(int poolSize)
  {
    if (poolSize == 0) {
      return new ReferenceCountingResourceHolder<>(ProcessingBuffersSet.EMPTY, () -> {});
    }

    final long heapMemoryPerWorker = heapMemoryToUse / taskCapacity;
    final int numThreadsPerWorker = (int) Math.min(
        numThreads,
        heapMemoryPerWorker / MIN_BUFFER_SIZE
    );

    if (numThreadsPerWorker < 1) {
      // Should not happen unless the CliIndexer has an unreasonable configuration.
      // CliIndexer typically has well in excess of 1 MB (min buffer size) of heap per task.
      throw new ISE("Cannot acquire buffers, available heap memory is not enough for task capacity[%d]", taskCapacity);
    }

    // bufferPools has one list per "poolSize"; each of those lists has "bufferCount" buffers of size "sliceSize".
    final List<List<ByteBuffer>> bufferPools = new ArrayList<>(poolSize);
    final int sliceSize = (int) Math.min(Integer.MAX_VALUE, heapMemoryPerWorker / numThreadsPerWorker);

    for (int i = 0; i < poolSize; i++) {
      final List<ByteBuffer> bufferPool = new ArrayList<>(numThreadsPerWorker);
      bufferPools.add(bufferPool);

      for (int j = 0; j < numThreadsPerWorker; j++) {
        bufferPool.add(ByteBuffer.allocate(sliceSize));
      }
    }

    // bufferPools is built, return it as a ProcessingBuffersSet.
    return new ReferenceCountingResourceHolder<>(
        ProcessingBuffersSet.fromCollection(bufferPools),
        () -> {} // Garbage collection will reclaim the buffers, since they are on-heap
    );
  }
}
