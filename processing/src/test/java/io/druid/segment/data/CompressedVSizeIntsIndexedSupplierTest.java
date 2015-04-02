/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.guava.CloseQuietly;
import io.druid.segment.CompressedPools;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class CompressedVSizeIntsIndexedSupplierTest extends CompressionStrategyTest
{
  private static final int[] MAX_VALUES = new int[] { 0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF };

  public CompressedVSizeIntsIndexedSupplierTest(CompressedObjectStrategy.CompressionStrategy compressionStrategy)
  {
    super(compressionStrategy);
  }

  private IndexedInts indexed;
  private CompressedVSizeIntsIndexedSupplier supplier;
  private int[] vals;

  @Before
  public void setUp() throws Exception
  {
    CloseQuietly.close(indexed);
    indexed = null;
    supplier = null;
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    CloseQuietly.close(indexed);
  }

  private void setupSimple(final int chunkSize)
  {
    vals = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16};

    supplier = CompressedVSizeIntsIndexedSupplier.fromList(
        Ints.asList(vals),
        Ints.max(vals),
        chunkSize,
        ByteOrder.nativeOrder(),
        compressionStrategy
    );

    indexed = supplier.get();
  }

  private void setupSimpleWithSerde(final int chunkSize) throws IOException
  {
    vals = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16};

    makeWithSerde(chunkSize);
  }

  private void makeWithSerde(final int chunkSize) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final CompressedVSizeIntsIndexedSupplier theSupplier = CompressedVSizeIntsIndexedSupplier.fromList(
        Ints.asList(vals), Ints.max(vals), chunkSize, ByteOrder.nativeOrder(), compressionStrategy
    );
    theSupplier.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(theSupplier.getSerializedSize(), bytes.length);

    supplier = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(ByteBuffer.wrap(bytes), ByteOrder.nativeOrder());
    indexed = supplier.get();
  }

  private void setupLargeChunks(final int chunkSize, final int totalSize, final int maxValue) throws IOException
  {
    vals = new int[totalSize];
    Random rand = new Random(0);
    for(int i = 0; i < vals.length; ++i) {
      // VSizeIndexed only allows positive values
      vals[i] = Math.abs(rand.nextInt(maxValue + 1));
    }

    makeWithSerde(chunkSize);
  }

  @Test
  public void testSanity() throws Exception
  {
    setupSimple(5);

    Assert.assertEquals(4, supplier.getBaseBuffers().size());
    assertIndexMatchesVals();

    // test powers of 2
    setupSimple(4);
    Assert.assertEquals(4, supplier.getBaseBuffers().size());
    assertIndexMatchesVals();

    setupSimple(32);
    Assert.assertEquals(1, supplier.getBaseBuffers().size());
    assertIndexMatchesVals();
  }

  @Test
  public void testLargeChunks() throws Exception
  {
    for (int maxValue : MAX_VALUES) {
      System.out.println(maxValue);

      final int maxChunkSize = CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue);

      setupLargeChunks(maxChunkSize, 10 * maxChunkSize, maxValue);
      Assert.assertEquals(10, supplier.getBaseBuffers().size());
      assertIndexMatchesVals();

      setupLargeChunks(maxChunkSize, 10 * maxChunkSize + 1, maxValue);
      Assert.assertEquals(11, supplier.getBaseBuffers().size());
      assertIndexMatchesVals();

      setupLargeChunks(maxChunkSize - 1, 10 * (maxChunkSize - 1) + 1, maxValue);
      Assert.assertEquals(11, supplier.getBaseBuffers().size());
      assertIndexMatchesVals();
    }
  }

  @Test
  public void testChunkTooBig() throws Exception
  {
    for(int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue);
      try {
        setupLargeChunks(maxChunkSize + 1, 10 * (maxChunkSize + 1), maxValue);
        Assert.fail();
      } catch(IllegalArgumentException e) {
        Assert.assertTrue("chunk too big for maxValue " + maxValue, true);
      }
    }
  }

  @Test
  public void testmaxIntsInBuffer() throws Exception
  {
    Assert.assertEquals(CompressedPools.BUFFER_SIZE - 3, CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForBytes(1));
    Assert.assertEquals((CompressedPools.BUFFER_SIZE - 2) / 2, CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForBytes(2));
    Assert.assertEquals((CompressedPools.BUFFER_SIZE - 1) / 3, CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForBytes(3));
    Assert.assertEquals((CompressedPools.BUFFER_SIZE) / 4, CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForBytes(4));
  }

  @Test
  public void testSanityWithSerde() throws Exception
  {
    setupSimpleWithSerde(5);

    Assert.assertEquals(4, supplier.getBaseBuffers().size());
    assertIndexMatchesVals();
  }


  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  @Test
  public void testConcurrentThreadReads() throws Exception
  {
    setupSimple(5);

    final AtomicReference<String> reason = new AtomicReference<>("none");

    final int numRuns = 1000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch stopLatch = new CountDownLatch(2);
    final AtomicBoolean failureHappened = new AtomicBoolean(false);
    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          startLatch.await();
        }
        catch (InterruptedException e) {
          failureHappened.set(true);
          reason.set("interrupt.");
          stopLatch.countDown();
          return;
        }

        try {
          for (int i = 0; i < numRuns; ++i) {
            for (int j = 0; j < indexed.size(); ++j) {
              final long val = vals[j];
              final long indexedVal = indexed.get(j);
              if (Longs.compare(val, indexedVal) != 0) {
                failureHappened.set(true);
                reason.set(String.format("Thread1[%d]: %d != %d", j, val, indexedVal));
                stopLatch.countDown();
                return;
              }
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          failureHappened.set(true);
          reason.set(e.getMessage());
        }

        stopLatch.countDown();
      }
    }).start();

    final IndexedInts indexed2 = supplier.get();
    try {
      new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            startLatch.await();
          }
          catch (InterruptedException e) {
            stopLatch.countDown();
            return;
          }

          try {
            for (int i = 0; i < numRuns; ++i) {
              for (int j = indexed2.size() - 1; j >= 0; --j) {
                final long val = vals[j];
                final long indexedVal = indexed2.get(j);
                if (Longs.compare(val, indexedVal) != 0) {
                  failureHappened.set(true);
                  reason.set(String.format("Thread2[%d]: %d != %d", j, val, indexedVal));
                  stopLatch.countDown();
                  return;
                }
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
            reason.set(e.getMessage());
            failureHappened.set(true);
          }

          stopLatch.countDown();
        }
      }).start();

      startLatch.countDown();

      stopLatch.await();
    }
    finally {
      CloseQuietly.close(indexed2);
    }

    if (failureHappened.get()) {
      Assert.fail("Failure happened.  Reason: " + reason.get());
    }
  }

  private void assertIndexMatchesVals()
  {
    Assert.assertEquals(vals.length, indexed.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0; i < indexed.size(); ++i) {
      final int expected = vals[i];
      final int actual = indexed.get(i);
      Assert.assertEquals(expected, actual);
      indices[i] = i;
    }

    Collections.shuffle(Arrays.asList(indices));
    // random access
    for (int i = 0; i < indexed.size(); ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], indexed.get(k));
    }
  }
}
