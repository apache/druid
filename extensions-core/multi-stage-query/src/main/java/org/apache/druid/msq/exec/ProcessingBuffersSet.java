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
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.kernel.StageDefinition;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * Holds a set of {@link ProcessingBuffers} for a {@link Worker}. Acquired from {@link ProcessingBuffersProvider}.
 */
public class ProcessingBuffersSet
{
  public static final ProcessingBuffersSet EMPTY = new ProcessingBuffersSet(Collections.emptyList());

  private final BlockingQueue<ProcessingBuffers> pool;

  public ProcessingBuffersSet(Collection<ProcessingBuffers> buffers)
  {
    this.pool = new ArrayBlockingQueue<>(buffers.isEmpty() ? 1 : buffers.size());
    this.pool.addAll(buffers);
  }

  /**
   * Equivalent to calling {@link ProcessingBuffers#fromCollection} on each collection in the overall collection,
   * then creating an instance.
   */
  public static <T extends Collection<ByteBuffer>> ProcessingBuffersSet fromCollection(final Collection<T> processingBuffers)
  {
    return new ProcessingBuffersSet(
        processingBuffers.stream()
                         .map(ProcessingBuffers::fromCollection)
                         .collect(Collectors.toList())
    );
  }

  /**
   * Acquire buffers if a particular stages needs them; otherwise, returns a holder that throws an exception on
   * {@link ResourceHolder#get()}.
   */
  public ResourceHolder<ProcessingBuffers> acquireForStage(final StageDefinition stageDef)
  {
    if (!stageDef.getProcessor().usesProcessingBuffers()) {
      return new NilResourceHolder<>();
    } else {
      return acquire();
    }
  }

  /**
   * Acquire buffers unconditionally. In production, it is expected that callers will use
   * {@link #acquireForStage(StageDefinition)}.
   */
  public ResourceHolder<ProcessingBuffers> acquire()
  {
    final ProcessingBuffers buffers = pool.poll();

    if (buffers == null) {
      // Never happens, because the pool acquired from ProcessingBuffersProvider must be big enough for all
      // concurrent processing buffer needs. (In other words: if this does happen, it's a bug.)
      throw DruidException.defensive("Processing buffers not available");
    }

    return new ResourceHolder<>()
    {
      @Override
      public ProcessingBuffers get()
      {
        return buffers;
      }

      @Override
      public void close()
      {
        pool.add(buffers);
      }
    };
  }

  /**
   * Resource holder that throws an exception on {@link #get()}.
   */
  static class NilResourceHolder<T> implements ResourceHolder<T>
  {
    @Override
    public T get()
    {
      throw DruidException.defensive("Unexpected call to get()");
    }

    @Override
    public void close()
    {
      // Do nothing.
    }
  }
}
