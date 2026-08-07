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
  private static final int PROCESSING_THREADS = 4;
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
  public void test_acquire_singleSliceUsesFullChunk()
  {
    // With poolSize=1 and one merge buffer of BUFFER_SIZE, the chunk is BUFFER_SIZE.
    // Requesting 1 slice should give a single buffer of full chunk size and a Bouncer of 1.
    when(mockBufferHolder.get()).thenReturn(testBuffer);
    when(mockMergeBufferPool.takeBatch(eq(1), eq(TIMEOUT_MILLIS)))
        .thenReturn(List.of(mockBufferHolder));

    final ResourceHolder<ProcessingBuffersSet> result = provider.acquire(1, TIMEOUT_MILLIS);
    try {
      final ResourceHolder<ProcessingBuffers> holder = result.get().acquire(1);
      try {
        final ProcessingBuffers buffers = holder.get();
        Assert.assertEquals(1, buffers.getBouncer().getMaxCount());

        final ResourceHolder<ByteBuffer> sliceHolder = buffers.getBufferPool().take();
        Assert.assertEquals(BUFFER_SIZE, sliceHolder.get().capacity());
        sliceHolder.close();
      }
      finally {
        holder.close();
      }
    }
    finally {
      result.close();
    }
  }

  @Test
  public void test_acquire_processingThreadsSlices()
  {
    // Requesting PROCESSING_THREADS slices yields the maximum slicing: each slice is BUFFER_SIZE/PROCESSING_THREADS.
    when(mockBufferHolder.get()).thenReturn(testBuffer);
    when(mockMergeBufferPool.takeBatch(eq(1), eq(TIMEOUT_MILLIS)))
        .thenReturn(List.of(mockBufferHolder));

    final ResourceHolder<ProcessingBuffersSet> result = provider.acquire(1, TIMEOUT_MILLIS);
    try {
      final ResourceHolder<ProcessingBuffers> holder = result.get().acquire(PROCESSING_THREADS);
      try {
        final ProcessingBuffers buffers = holder.get();
        Assert.assertEquals(PROCESSING_THREADS, buffers.getBouncer().getMaxCount());

        final List<ResourceHolder<ByteBuffer>> sliceHolders = new ArrayList<>();
        try {
          for (int i = 0; i < PROCESSING_THREADS; i++) {
            final ResourceHolder<ByteBuffer> sliceHolder = buffers.getBufferPool().take();
            Assert.assertEquals(BUFFER_SIZE / PROCESSING_THREADS, sliceHolder.get().capacity());
            sliceHolders.add(sliceHolder);
          }
        }
        finally {
          for (final ResourceHolder<ByteBuffer> sh : sliceHolders) {
            sh.close();
          }
        }
      }
      finally {
        holder.close();
      }
    }
    finally {
      result.close();
    }
  }

  @Test
  public void test_acquire_resliceAfterRelease()
  {
    // Acquire with N=2, release, then re-acquire with N=4. The chunk should be re-sliced.
    when(mockBufferHolder.get()).thenReturn(testBuffer);
    when(mockMergeBufferPool.takeBatch(eq(1), eq(TIMEOUT_MILLIS)))
        .thenReturn(List.of(mockBufferHolder));

    final ResourceHolder<ProcessingBuffersSet> result = provider.acquire(1, TIMEOUT_MILLIS);
    try {
      // First acquisition with 2 slices.
      final ResourceHolder<ProcessingBuffers> holder1 = result.get().acquire(2);
      Assert.assertEquals(2, holder1.get().getBouncer().getMaxCount());
      Assert.assertEquals(BUFFER_SIZE / 2, holder1.get().getBufferPool().take().get().capacity());
      holder1.close();

      // Second acquisition with 4 slices — same chunk, different slicing.
      final ResourceHolder<ProcessingBuffers> holder2 = result.get().acquire(4);
      Assert.assertEquals(4, holder2.get().getBouncer().getMaxCount());
      Assert.assertEquals(BUFFER_SIZE / 4, holder2.get().getBufferPool().take().get().capacity());
      holder2.close();
    }
    finally {
      result.close();
    }
  }

  @Test
  public void test_acquire_poolSizeTwo()
  {
    when(mockBufferHolder.get()).thenReturn(testBuffer);
    when(mockMergeBufferPool.takeBatch(eq(1), eq(TIMEOUT_MILLIS)))
        .thenReturn(List.of(mockBufferHolder));

    final int poolSize = 2;
    final ResourceHolder<ProcessingBuffersSet> result = provider.acquire(poolSize, TIMEOUT_MILLIS);

    Assert.assertNotNull(result);
    final ProcessingBuffersSet buffersSet = result.get();
    Assert.assertNotNull(buffersSet);

    // Each slot's chunk has capacity BUFFER_SIZE/poolSize. Requesting PROCESSING_THREADS slices yields slices
    // of size (BUFFER_SIZE/poolSize)/PROCESSING_THREADS.
    for (int i = 0; i < poolSize; i++) {
      final ResourceHolder<ProcessingBuffers> buffersHolder = buffersSet.acquire(PROCESSING_THREADS);
      try {
        final ProcessingBuffers buffers = buffersHolder.get();
        Assert.assertEquals(PROCESSING_THREADS, buffers.getBouncer().getMaxCount());

        final int expectedSliceSize = BUFFER_SIZE / poolSize / PROCESSING_THREADS;
        final List<ResourceHolder<ByteBuffer>> resourceHolders = new ArrayList<>();
        for (int j = 0; j < PROCESSING_THREADS; j++) {
          final ResourceHolder<ByteBuffer> sliceHolder = buffers.getBufferPool().take();
          Assert.assertEquals(expectedSliceSize, sliceHolder.get().capacity());
          resourceHolders.add(sliceHolder);
        }
        for (final ResourceHolder<ByteBuffer> resourceHolder : resourceHolders) {
          resourceHolder.close();
        }
      }
      finally {
        buffersHolder.close();
      }
    }

    result.close();
  }

  @Test
  public void test_acquire_timeout()
  {
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
