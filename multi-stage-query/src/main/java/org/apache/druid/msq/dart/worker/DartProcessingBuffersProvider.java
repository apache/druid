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
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.utils.CloseableUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Production implementation of {@link ProcessingBuffersProvider} that uses the merge buffer pool. Each call
 * to {@link #acquire(int, long)} acquires one merge buffer and slices it up.
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
  public ResourceHolder<ProcessingBuffersSet> acquire(final int poolSize, final long timeoutMillis)
  {
    if (poolSize == 0) {
      return new ReferenceCountingResourceHolder<>(ProcessingBuffersSet.EMPTY, () -> {});
    }

    final List<ReferenceCountingResourceHolder<ByteBuffer>> batch = mergeBufferPool.takeBatch(1, timeoutMillis);
    if (batch.isEmpty()) {
      throw new MSQException(CanceledFault.timeout());
    }

    final ReferenceCountingResourceHolder<ByteBuffer> bufferHolder = batch.get(0);
    try {
      final ByteBuffer buffer = bufferHolder.get().duplicate();
      final int chunkSize = buffer.capacity() / poolSize;
      final List<ProcessingBuffersSet.Slot> slots = new ArrayList<>(poolSize);

      for (int i = 0; i < poolSize; i++) {
        buffer.position(chunkSize * i).limit(chunkSize * (i + 1));
        slots.add(new LazySlot(buffer.slice(), processingThreads));
      }

      return new ReferenceCountingResourceHolder<>(new ProcessingBuffersSet(slots), bufferHolder);
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, bufferHolder);
    }
  }

  /**
   * Lazy slot that holds one chunk of the shared merge buffer and slices it on demand to match the stage's
   * actual concurrent-processor count.
   */
  static final class LazySlot implements ProcessingBuffersSet.Slot
  {
    private final ByteBuffer chunk;
    private final int maxSlices;

    LazySlot(final ByteBuffer chunk, final int maxSlices)
    {
      this.chunk = chunk;
      this.maxSlices = maxSlices;
    }

    @Override
    public ProcessingBuffers acquire(final int requestedSlices)
    {
      if (requestedSlices > maxSlices) {
        throw DruidException.defensive(
            "requestedSlices[%d] too large for maxSlices[%d]",
            requestedSlices,
            maxSlices
        );
      }

      if (requestedSlices < 1) {
        throw DruidException.defensive("requestedSlices[%d] must be positive", requestedSlices);
      }

      final int sliceSize = chunk.capacity() / requestedSlices;
      final BlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<>(requestedSlices);
      final ByteBuffer working = chunk.duplicate();
      for (int j = 0; j < requestedSlices; j++) {
        working.position(sliceSize * j).limit(sliceSize * (j + 1));
        queue.add(working.slice());
      }
      return new ProcessingBuffers(new QueueNonBlockingPool<>(queue), new Bouncer(requestedSlices));
    }
  }
}
