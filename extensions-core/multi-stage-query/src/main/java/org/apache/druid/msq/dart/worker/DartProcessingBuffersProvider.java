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

package org.apache.druid.msq.dart.worker;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.QueueNonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.msq.exec.ProcessingBuffers;
import org.apache.druid.msq.exec.ProcessingBuffersProvider;
import org.apache.druid.msq.exec.ProcessingBuffersSet;
import org.apache.druid.utils.CloseableUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Production implementation of {@link ProcessingBuffersProvider} that uses the merge buffer pool. Each call
 * to {@link #acquire(int)} acquires one merge buffer and slices it up.
 */
public class DartProcessingBuffersProvider implements ProcessingBuffersProvider
{
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final int processingThreads;

  public DartProcessingBuffersProvider(BlockingPool<ByteBuffer> mergeBufferPool, int processingThreads)
  {
    this.mergeBufferPool = mergeBufferPool;
    this.processingThreads = processingThreads;
  }

  @Override
  public ResourceHolder<ProcessingBuffersSet> acquire(final int poolSize)
  {
    if (poolSize == 0) {
      return new ReferenceCountingResourceHolder<>(ProcessingBuffersSet.EMPTY, () -> {});
    }

    final List<ReferenceCountingResourceHolder<ByteBuffer>> batch = mergeBufferPool.takeBatch(1, 0);
    if (batch.isEmpty()) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("No merge buffers available, cannot execute query");
    }

    final ReferenceCountingResourceHolder<ByteBuffer> bufferHolder = batch.get(0);
    try {
      final ByteBuffer buffer = bufferHolder.get().duplicate();
      final int sliceSize = buffer.capacity() / poolSize / processingThreads;
      final List<ProcessingBuffers> pool = new ArrayList<>(poolSize);

      for (int i = 0; i < poolSize; i++) {
        final BlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<>(processingThreads);
        for (int j = 0; j < processingThreads; j++) {
          final int sliceNum = i * processingThreads + j;
          buffer.position(sliceSize * sliceNum).limit(sliceSize * (sliceNum + 1));
          queue.add(buffer.slice());
        }
        final ProcessingBuffers buffers = new ProcessingBuffers(
            new QueueNonBlockingPool<>(queue),
            new Bouncer(processingThreads)
        );
        pool.add(buffers);
      }

      return new ReferenceCountingResourceHolder<>(new ProcessingBuffersSet(pool), bufferHolder);
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, bufferHolder);
    }
  }
}
