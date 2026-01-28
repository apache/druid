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

import org.junit.Assert;
import org.junit.Test;

public class AppendableMemoryTest
{

  @Test
  public void testReserveAdditionalWithLargeLastBlockAndSmallAllocator()
  {
    /*
    This tests the edge case when the last chunk of memory allocated by the allocator has greater available free space
    than what can be allocated by the allocator. In that case, the availableToReserve call should return the free memory from
    the last allocated block, and reserveAdditional when called with that value should return true (and not do any additional
    allocation). This test case assumes a lot about the implementation of the AppendableMemory, but a lot of the assertions made
    in this test are logical, and should hold true. The final assertion is the most important which checks that the free space
    in the last block takes precedence over the memory allocator, which should hold true irrespective of the implementation
     */

    // Allocator that can allocate atmost 100 bytes and AppendableMemory created with that allocator
    MemoryAllocator memoryAllocator = new HeapMemoryAllocator(100);
    AppendableMemory appendableMemory = AppendableMemory.create(memoryAllocator, 10);

    // Reserves a chunk of 10 bytes. The call should return true since the allocator can allocate 100 bytes
    Assert.assertTrue(appendableMemory.reserveAdditional(10));

    // Last block is empty, the appendable memory is essentially empty
    Assert.assertEquals(100, appendableMemory.availableToReserve());

    // Advance the cursor so that it is not treated as empty chunk
    appendableMemory.advanceCursor(4);

    // We should be able to use the remaining 90 bytes from the allocator
    Assert.assertEquals(90, appendableMemory.availableToReserve());

    // Reserve a chunk of 80 bytes, and advance the cursor so that it is not treated as an empty chunk
    Assert.assertTrue(appendableMemory.reserveAdditional(80));
    appendableMemory.advanceCursor(4);

    // At this point, we have 2 chunks with the following (used:free) / total statistics
    // chunk0 - (4:6)/10
    // chunk1 - (4:76)/80
    // The allocator still has 10 additional bytes to reserve

    // Even though the allocator has only 10 bytes, the last chunk has 76 free bytes which can be used. That would take precedence
    // since that is a larger number
    Assert.assertEquals(76, appendableMemory.availableToReserve());

    // This assertion must always be true irrespective of the internal implementation
    Assert.assertTrue(appendableMemory.reserveAdditional(appendableMemory.availableToReserve()));
  }

}
