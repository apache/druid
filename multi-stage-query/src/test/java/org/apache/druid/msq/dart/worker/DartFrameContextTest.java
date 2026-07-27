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

package org.apache.druid.msq.dart.worker;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.exec.ProcessingBuffers;
import org.apache.druid.msq.exec.ProcessingBuffersSet;
import org.apache.druid.msq.kernel.StageId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DartFrameContextTest
{
  private static final StageId STAGE_ID = new StageId("query-1", 0);

  private ProcessingBuffersSet buffersSet;

  @Before
  public void setUp()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(1024);
    buffersSet = ProcessingBuffersSet.fromCollection(ImmutableList.of(ImmutableList.of(buffer)));
  }

  @Test
  public void test_acquireProcessingBuffers_nullSet_throws()
  {
    final DartFrameContext context = makeContext(null);

    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> context.acquireProcessingBuffers(1)
    );

    Assert.assertEquals(
        "Stage[" + STAGE_ID + "] does not use processing buffers",
        e.getMessage()
    );
  }

  @Test
  public void test_acquireProcessingBuffers_alreadyAcquired_throws()
  {
    final DartFrameContext context = makeContext(buffersSet);
    context.acquireProcessingBuffers(1);

    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> context.acquireProcessingBuffers(1)
    );

    Assert.assertEquals("Processing buffers already acquired", e.getMessage());

    context.close();
  }

  @Test
  public void test_processingBuffers_notAcquired_throws()
  {
    final DartFrameContext context = makeContext(buffersSet);

    final DruidException e = Assert.assertThrows(
        DruidException.class,
        context::processingBuffers
    );

    Assert.assertEquals("Processing buffers not yet acquired", e.getMessage());
  }

  @Test
  public void test_processingBuffers_afterAcquire_returnsBuffers()
  {
    final DartFrameContext context = makeContext(buffersSet);
    context.acquireProcessingBuffers(1);

    final ProcessingBuffers buffers = context.processingBuffers();
    Assert.assertNotNull(buffers);
    Assert.assertNotNull(buffers.getBufferPool());
    Assert.assertNotNull(buffers.getBouncer());

    context.close();
  }

  @Test
  public void test_close_withoutAcquire_isNoop()
  {
    final DartFrameContext context = makeContext(buffersSet);

    // Should not throw.
    context.close();

    // Slot was never acquired, so it should still be available.
    buffersSet.acquire(1).close();
  }

  @Test
  public void test_close_afterAcquire_releasesSlot()
  {
    final DartFrameContext context = makeContext(buffersSet);
    context.acquireProcessingBuffers(1);

    context.close();

    // Slot should now be back in the pool and re-acquirable.
    buffersSet.acquire(1).close();
  }

  private static DartFrameContext makeContext(final ProcessingBuffersSet processingBuffersSet)
  {
    return new DartFrameContext(
        STAGE_ID,
        null,
        null,
        null,
        null,
        null,
        null,
        processingBuffersSet,
        null,
        null,
        null
    );
  }
}
