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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.ResourceHolder;

import java.util.Optional;

/**
 * Allocator of WritableMemory. Not thread safe.
 */
public interface MemoryAllocator
{
  /**
   * Allocates a block of memory of capacity {@param size}. Returns empty if no more memory is available.
   * The memory can be freed by closing the returned {@link ResourceHolder}.
   *
   * The returned WritableMemory object will use little-endian byte order.
   */
  Optional<ResourceHolder<WritableMemory>> allocate(long size);

  /**
   * Returns the number of bytes available for allocations.
   *
   * May return {@link Long#MAX_VALUE} if there is no inherent limit. This generally does not mean you can actually
   * allocate 9 exabytes.
   */
  long available();

  /**
   * Returns the number of bytes managed by this allocator. When no memory has been allocated yet, this is identical to
   * {@link #available()}.
   *
   * May return {@link Long#MAX_VALUE} if there is no inherent limit. This generally does not mean you can actually
   * allocate 9 exabytes.
   */
  long capacity();
}
