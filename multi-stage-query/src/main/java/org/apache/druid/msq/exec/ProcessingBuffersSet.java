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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * Holds a set of {@link Slot}, each of which can produce {@link ProcessingBuffers} for one concurrent stage.
 * Acquired from {@link ProcessingBuffersProvider}.
 *
 * Slots come in two flavors:
 * <ul>
 *   <li>{@link EagerSlot}: holds an already-built {@link ProcessingBuffers}; ignores the requested slice count.
 *       Used by buffer providers that pre-allocate (Peon, Indexer).</li>
 *   <li>Lazy slots (provider-defined): hold a buffer chunk and slice it per stage based on the actual concurrent
 *       processor count, so a stage that runs fewer processors gets larger slices. Used by Dart.</li>
 * </ul>
 */
public class ProcessingBuffersSet
{
  public static final ProcessingBuffersSet EMPTY = new ProcessingBuffersSet(Collections.emptyList());

  private final BlockingQueue<Slot> pool;

  public ProcessingBuffersSet(final Collection<? extends Slot> slots)
  {
    this.pool = new ArrayBlockingQueue<>(slots.isEmpty() ? 1 : slots.size());
    this.pool.addAll(slots);
  }

  /**
   * Wrap a collection of pre-built {@link ProcessingBuffers}.
   */
  public static ProcessingBuffersSet wrap(final Collection<ProcessingBuffers> buffers)
  {
    return new ProcessingBuffersSet(buffers.stream().map(EagerSlot::new).collect(Collectors.toList()));
  }

  /**
   * Equivalent to calling {@link ProcessingBuffers#fromCollection} on each collection in the overall collection,
   * then wrapping in eager slots.
   */
  public static <T extends Collection<ByteBuffer>> ProcessingBuffersSet fromCollection(final Collection<T> processingBuffers)
  {
    return wrap(
        processingBuffers.stream()
                         .map(ProcessingBuffers::fromCollection)
                         .collect(Collectors.toList())
    );
  }

  /**
   * Acquire buffers with a specific requested slice count. The actual number of slices may be higher but will
   * not be lower.
   */
  public ResourceHolder<ProcessingBuffers> acquire(final int requestedSlices)
  {
    final Slot slot = pool.poll();

    if (slot == null) {
      // Never happens, because the pool acquired from ProcessingBuffersProvider must be big enough for all
      // concurrent processing buffer needs. (In other words: if this does happen, it's a bug.)
      throw DruidException.defensive("Processing buffers not available");
    }

    final ProcessingBuffers buffers;
    try {
      buffers = slot.acquire(requestedSlices);
    }
    catch (Throwable t) {
      pool.add(slot);
      throw t;
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
        pool.add(slot);
      }
    };
  }

  /**
   * A producer of {@link ProcessingBuffers} from a single concurrent-stage slot in the pool. Implementations
   * decide whether the slice count argument to {@link #acquire} is honored (lazy slots) or ignored (eager slots).
   */
  public interface Slot
  {
    /**
     * Produce a {@link ProcessingBuffers} suitable for a stage that will run up to {@code requestedSlices}
     * concurrent processors. Implementations may choose to ignore the argument when the slot's buffers are
     * already laid out (e.g., {@link EagerSlot}).
     */
    ProcessingBuffers acquire(int requestedSlices);
  }

  /**
   * Slot that wraps an already-built {@link ProcessingBuffers}.
   */
  public static final class EagerSlot implements Slot
  {
    private final ProcessingBuffers buffers;

    public EagerSlot(final ProcessingBuffers buffers)
    {
      this.buffers = buffers;
    }

    @Override
    public ProcessingBuffers acquire(final int requestedSlices)
    {
      if (requestedSlices > buffers.getBouncer().getMaxCount()) {
        throw DruidException.defensive(
            "requestedSlices[%d] too large, only have[%d] buffers",
            requestedSlices,
            buffers.getBouncer().getMaxCount()
        );
      }

      return buffers;
    }
  }
}
