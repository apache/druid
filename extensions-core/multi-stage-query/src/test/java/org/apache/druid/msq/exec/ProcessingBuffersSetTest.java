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

import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.utils.CloseableUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ProcessingBuffersSetTest
{
  @Test
  public void test_empty_acquire()
  {
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        ProcessingBuffersSet.EMPTY::acquire
    );

    Assert.assertEquals("Processing buffers not available", e.getMessage());
  }

  @Test
  public void test_fromCollection() throws IOException
  {
    // Create byte buffers
    final ByteBuffer buffer1 = ByteBuffer.allocate(1024);
    final ByteBuffer buffer2 = ByteBuffer.allocate(1024);
    final ByteBuffer buffer3 = ByteBuffer.allocate(1024);

    final List<ByteBuffer> bufferList1 = ImmutableList.of(buffer1);
    final List<ByteBuffer> bufferList2 = ImmutableList.of(buffer2);
    final List<ByteBuffer> bufferList3 = ImmutableList.of(buffer3);

    final List<List<ByteBuffer>> bufferLists = ImmutableList.of(bufferList1, bufferList2, bufferList3);

    final ProcessingBuffersSet buffersSet = ProcessingBuffersSet.fromCollection(bufferLists);

    // Should be able to acquire all three
    final ResourceHolder<ProcessingBuffers> holder1 = buffersSet.acquire();
    final ResourceHolder<ProcessingBuffers> holder2 = buffersSet.acquire();
    final ResourceHolder<ProcessingBuffers> holder3 = buffersSet.acquire();

    Assert.assertNotNull(holder1.get());
    Assert.assertNotNull(holder2.get());
    Assert.assertNotNull(holder3.get());

    // Verify each has a buffer pool and bouncer
    Assert.assertNotNull(holder1.get().getBufferPool());
    Assert.assertNotNull(holder1.get().getBouncer());
    Assert.assertNotNull(holder2.get().getBufferPool());
    Assert.assertNotNull(holder2.get().getBouncer());
    Assert.assertNotNull(holder3.get().getBufferPool());
    Assert.assertNotNull(holder3.get().getBouncer());

    // Clean up
    CloseableUtils.closeAll(holder1, holder2, holder3);
  }

  @Test
  public void test_nilResourceHolder()
  {
    final ProcessingBuffersSet.NilResourceHolder<Object> nilHolder = new ProcessingBuffersSet.NilResourceHolder<>();

    final DruidException e = Assert.assertThrows(
        DruidException.class,
        nilHolder::get
    );

    Assert.assertEquals("Unexpected call to get()", e.getMessage());

    nilHolder.close(); // Should do nothing
  }
}
