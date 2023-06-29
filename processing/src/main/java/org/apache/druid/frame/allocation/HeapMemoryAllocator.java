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

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Allocator that uses {@link ByteBuffer#allocate} to create chunks in the JVM heap.
 */
public class HeapMemoryAllocator implements MemoryAllocator
{
  private final long capacity;

  private long bytesAllocated = 0;

  private HeapMemoryAllocator(final long capacity)
  {
    this.capacity = capacity;
  }

  /**
   * Create an allocator that is "unlimited", which, of course, means it is limited only by available JVM heap.
   */
  public static HeapMemoryAllocator unlimited()
  {
    return new HeapMemoryAllocator(Long.MAX_VALUE);
  }

  @Override
  public Optional<ResourceHolder<WritableMemory>> allocate(final long size)
  {
    if (bytesAllocated < capacity - size) {
      bytesAllocated += size;

      return Optional.of(
          new ResourceHolder<WritableMemory>()
          {
            private WritableMemory memory =
                WritableMemory.writableWrap(ByteBuffer.allocate(Ints.checkedCast(size)).order(ByteOrder.LITTLE_ENDIAN));

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
              bytesAllocated -= size;
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
    return capacity - bytesAllocated;
  }

  @Override
  public long capacity()
  {
    return capacity;
  }
}
