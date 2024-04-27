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

import org.apache.druid.java.util.common.ISE;

/**
 * Wraps a single {@link MemoryAllocator}.
 *
 * The same instance is returned on each call to {@link #newAllocator()}, after validating that it is 100% free.
 * Calling {@link #newAllocator()} before freeing all previously-allocated memory leads to an IllegalStateException.
 */
public class SingleMemoryAllocatorFactory implements MemoryAllocatorFactory
{
  private final MemoryAllocator allocator;
  private final long capacity;

  public SingleMemoryAllocatorFactory(final MemoryAllocator allocator)
  {
    this.allocator = allocator;
    this.capacity = allocator.capacity();
  }

  @Override
  public MemoryAllocator newAllocator()
  {
    // Allocators are reused, which allows each call to "newAllocator" to use the same arena (if it's arena-based).
    // Just need to validate that it has actually been closed out prior to handing it to someone else.

    if (allocator.available() != allocator.capacity()) {
      throw new ISE("Allocator in use");
    }

    return allocator;
  }

  @Override
  public long allocatorCapacity()
  {
    return capacity;
  }
}
