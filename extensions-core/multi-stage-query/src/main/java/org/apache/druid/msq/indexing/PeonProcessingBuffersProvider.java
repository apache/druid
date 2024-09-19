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

import org.apache.druid.cli.CliPeon;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.exec.ProcessingBuffersProvider;
import org.apache.druid.msq.exec.ProcessingBuffersSet;
import org.apache.druid.utils.CloseableUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Production implementation of {@link ProcessingBuffersProvider} for tasks in {@link CliPeon}.
 */
public class PeonProcessingBuffersProvider implements ProcessingBuffersProvider
{
  private final AtomicBoolean acquired = new AtomicBoolean(false);
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private final int bufferCount;

  public PeonProcessingBuffersProvider(
      final NonBlockingPool<ByteBuffer> bufferPool,
      final int bufferCount
  )
  {
    this.bufferPool = bufferPool;
    this.bufferCount = bufferCount;
  }

  @Override
  public ResourceHolder<ProcessingBuffersSet> acquire(int poolSize)
  {
    if (poolSize == 0) {
      return new ReferenceCountingResourceHolder<>(ProcessingBuffersSet.EMPTY, () -> {});
    }

    if (!acquired.compareAndSet(false, true)) {
      // We expect a single task in the JVM for CliPeon.
      throw DruidException.defensive("Expected a single call to acquire() for[%s]", getClass().getName());
    }

    final Closer closer = Closer.create();

    try {
      // bufferPools has one list per "poolSize"; each of those lists has "bufferCount" buffers.
      // Build these by acquiring "bufferCount" processing buffers and slicing each one up into "poolSize" slices.
      final List<List<ByteBuffer>> bufferPools = new ArrayList<>();
      for (int i = 0; i < poolSize; i++) {
        bufferPools.add(new ArrayList<>(bufferCount));
      }

      for (int i = 0; i < bufferCount; i++) {
        final ResourceHolder<ByteBuffer> bufferHolder = closer.register(bufferPool.take());
        final ByteBuffer buffer = bufferHolder.get().duplicate();
        final int sliceSize = buffer.capacity() / poolSize;

        for (int j = 0; j < poolSize; j++) {
          buffer.position(sliceSize * j).limit(sliceSize * (j + 1));
          bufferPools.get(j).add(buffer.slice());
        }
      }

      // bufferPools is built, return it as a ProcessingBuffersSet.
      return new ReferenceCountingResourceHolder<>(
          ProcessingBuffersSet.fromCollection(bufferPools),
          closer
      );
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, closer);
    }
  }
}
