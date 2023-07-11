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

package org.apache.druid.frame.allocation;

import com.google.common.base.Preconditions;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Allocator that uses a reusable {@link WritableMemory} arena. The allocator maintains a high watermark that
 * is reset to zero when all outstanding allocations have been freed.
 */
public class ArenaMemoryAllocator implements MemoryAllocator
{
  private final WritableMemory arena;
  private long allocations = 0;
  private long position = 0;

  private ArenaMemoryAllocator(WritableMemory arena)
  {
    this.arena = Preconditions.checkNotNull(arena, "arena");
  }

  /**
   * Creates an allocator based on a specific {@link ByteBuffer}. The buffer is never freed, so to ensure proper
   * cleanup when the allocator is discarded, this buffer must be on-heap (so it can be garbage collected) rather
   * than off-heap.
   */
  public static ArenaMemoryAllocator create(final ByteBuffer buffer)
  {
    return new ArenaMemoryAllocator(WritableMemory.writableWrap(buffer.slice()));
  }

  /**
   * Creates an allocator of a specific size using an on-heap {@link ByteBuffer}.
   */
  public static ArenaMemoryAllocator createOnHeap(final int capacity)
  {
    return create(ByteBuffer.allocate(capacity));
  }

  @Override
  public Optional<ResourceHolder<WritableMemory>> allocate(final long size)
  {
    if (position + size < arena.getCapacity()) {
      final long start = position;
      allocations++;
      position += size;

      return Optional.of(
          new ResourceHolder<WritableMemory>()
          {
            private WritableMemory memory = arena.writableRegion(start, size, ByteOrder.LITTLE_ENDIAN);

            @Override
            public WritableMemory get()
            {
              if (memory == null) {
                throw new ISE("Already closed");
              }

              return memory;
            }

            @Override
            public void close()
            {
              memory = null;

              if (--allocations == 0) {
                // All allocations closed; reset position to enable arena reuse.
                position = 0;
              }
            }
          }
      );
    } else {
      return Optional.empty();
    }
  }

  @Override
  public long available()
  {
    return arena.getCapacity() - position;
  }

  @Override
  public long capacity()
  {
    return arena.getCapacity();
  }
}
