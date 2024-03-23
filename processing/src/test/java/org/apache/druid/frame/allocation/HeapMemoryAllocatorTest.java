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
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class HeapMemoryAllocatorTest
{
  private static final int ALLOCATOR_SIZE = 10;

  @Test
  public void testAllocationInSinglePass()
  {
    MemoryAllocator heapMemoryAllocator = new HeapMemoryAllocator(ALLOCATOR_SIZE);
    Optional<ResourceHolder<WritableMemory>> memoryResourceHolderOptional = heapMemoryAllocator.allocate(ALLOCATOR_SIZE);
    Assert.assertTrue(memoryResourceHolderOptional.isPresent());
    ResourceHolder<WritableMemory> memoryResourceHolder = memoryResourceHolderOptional.get();
    WritableMemory memory = memoryResourceHolder.get();
    for (int i = 0; i < ALLOCATOR_SIZE; ++i) {
      memory.putByte(i, (byte) 0xFF);
    }
  }

  @Test
  public void testAllocationInMultiplePasses()
  {
    MemoryAllocator heapMemoryAllocator = new HeapMemoryAllocator(ALLOCATOR_SIZE);

    Optional<ResourceHolder<WritableMemory>> memoryResourceHolderOptional1 = heapMemoryAllocator.allocate(ALLOCATOR_SIZE
                                                                                                          - 4);
    Assert.assertTrue(memoryResourceHolderOptional1.isPresent());
    ResourceHolder<WritableMemory> memoryResourceHolder1 = memoryResourceHolderOptional1.get();
    WritableMemory memory1 = memoryResourceHolder1.get();

    Optional<ResourceHolder<WritableMemory>> memoryResourceHolderOptional2 = heapMemoryAllocator.allocate(4);
    Assert.assertTrue(memoryResourceHolderOptional2.isPresent());
    ResourceHolder<WritableMemory> memoryResourceHolder2 = memoryResourceHolderOptional2.get();
    WritableMemory memory2 = memoryResourceHolder2.get();

    for (int i = 0; i < ALLOCATOR_SIZE - 4; ++i) {
      memory1.putByte(i, (byte) 0xFF);
    }
    for (int i = 0; i < 4; ++i) {
      memory2.putByte(i, (byte) 0xFE);
    }
    // Readback to ensure that value hasn't been overwritten
    for (int i = 0; i < ALLOCATOR_SIZE - 4; ++i) {
      Assert.assertEquals((byte) 0xFF, memory1.getByte(i));
    }
    for (int i = 0; i < 4; ++i) {
      Assert.assertEquals((byte) 0xFE, memory2.getByte(i));
    }
  }

  @Test
  public void testOverallocationInSinglePass()
  {
    MemoryAllocator heapMemoryAllocator = new HeapMemoryAllocator(ALLOCATOR_SIZE);
    Optional<ResourceHolder<WritableMemory>> memoryResourceHolderOptional =
        heapMemoryAllocator.allocate(ALLOCATOR_SIZE + 1);
    Assert.assertFalse(memoryResourceHolderOptional.isPresent());
  }

  @Test
  public void testOverallocationInMultiplePasses()
  {
    MemoryAllocator heapMemoryAllocator = new HeapMemoryAllocator(ALLOCATOR_SIZE);
    Optional<ResourceHolder<WritableMemory>> memoryResourceHolderOptional =
        heapMemoryAllocator.allocate(ALLOCATOR_SIZE - 4);
    Assert.assertTrue(memoryResourceHolderOptional.isPresent());
    Assert.assertFalse(heapMemoryAllocator.allocate(5).isPresent());
  }

}
