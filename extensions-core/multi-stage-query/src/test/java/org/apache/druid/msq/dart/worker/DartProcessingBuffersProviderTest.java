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

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.msq.exec.ProcessingBuffers;
import org.apache.druid.msq.exec.ProcessingBuffersSet;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DartProcessingBuffersProviderTest
{
  private static final int PROCESSING_THREADS = 2;
  private static final long TIMEOUT_MILLIS = 1000L;
  private static final int BUFFER_SIZE = 1024;

  @Mock
  private BlockingPool<ByteBuffer> mockMergeBufferPool;

  @Mock
  private ReferenceCountingResourceHolder<ByteBuffer> mockBufferHolder;

  private DartProcessingBuffersProvider provider;
  private ByteBuffer testBuffer;

  @Before
  public void setUp()
  {
    provider = new DartProcessingBuffersProvider(mockMergeBufferPool, PROCESSING_THREADS);
    testBuffer = ByteBuffer.allocate(BUFFER_SIZE);
  }

  @Test
  public void test_acquire_poolSizeZero()
  {
    final ResourceHolder<ProcessingBuffersSet> result = provider.acquire(0, TIMEOUT_MILLIS);

    Assert.assertNotNull(result);
    Assert.assertEquals(ProcessingBuffersSet.EMPTY, result.get());

    // Should be able to close without issues
    result.close();
  }

  @Test
  public void test_acquire_poolSizeTwo()
  {
    // Setup mock to return a buffer
    when(mockBufferHolder.get()).thenReturn(testBuffer);
    when(mockMergeBufferPool.takeBatch(eq(1), eq(TIMEOUT_MILLIS)))
        .thenReturn(List.of(mockBufferHolder));

    // Test successful acquisition
    final int poolSize = 2;
    final ResourceHolder<ProcessingBuffersSet> result = provider.acquire(poolSize, TIMEOUT_MILLIS);

    Assert.assertNotNull(result);
    final ProcessingBuffersSet buffersSet = result.get();
    Assert.assertNotNull(buffersSet);

    // Verify we can acquire buffers from the set
    for (int i = 0; i < poolSize; i++) {
      final ResourceHolder<ProcessingBuffers> buffersHolder = buffersSet.acquire();
      Assert.assertNotNull(buffersHolder);

      final ProcessingBuffers buffers = buffersHolder.get();
      Assert.assertNotNull(buffers);
      Assert.assertNotNull(buffers.getBufferPool());
      Assert.assertNotNull(buffers.getBouncer());

      // The bouncer should have the correct max count (PROCESSING_THREADS)
      Assert.assertEquals(PROCESSING_THREADS, buffers.getBouncer().getMaxCount());

      // Verify that we can get processing threads number of buffers
      final List<ResourceHolder<ByteBuffer>> resourceHolders = new ArrayList<>();
      for (int j = 0; j < PROCESSING_THREADS; j++) {
        final ResourceHolder<ByteBuffer> bufferResource = buffers.getBufferPool().take();
        Assert.assertNotNull(bufferResource);
        Assert.assertNotNull(bufferResource.get());
        resourceHolders.add(bufferResource);
      }

      for (final ResourceHolder<ByteBuffer> resourceHolder : resourceHolders) {
        resourceHolder.close();
      }

      buffersHolder.close(); // Return to pool
    }

    result.close();
  }

  @Test
  public void test_acquire_timeout()
  {
    // Setup mock pool to return empty list (as happens during a timeout)
    when(mockMergeBufferPool.takeBatch(eq(1), eq(TIMEOUT_MILLIS)))
        .thenReturn(Collections.emptyList());

    MSQException exception = Assert.assertThrows(
        MSQException.class,
        () -> provider.acquire(1, TIMEOUT_MILLIS)
    );

    Assert.assertTrue(exception.getFault() instanceof CanceledFault);
    Assert.assertEquals("Canceled", exception.getFault().getErrorCode());
  }
}
