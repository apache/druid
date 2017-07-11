/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.CompressedPools;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.Channels;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class CompressedIntsIndexedSupplierTest extends CompressionStrategyTest
{
  public CompressedIntsIndexedSupplierTest(CompressedObjectStrategy.CompressionStrategy compressionStrategy)
  {
    super(compressionStrategy);
  }

  private IndexedInts indexed;
  private CompressedIntsIndexedSupplier supplier;
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
    CloseQuietly.close(indexed);

    vals = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16};

    supplier = CompressedIntsIndexedSupplier.fromIntBuffer(
        IntBuffer.wrap(vals),
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
    CloseQuietly.close(indexed);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final CompressedIntsIndexedSupplier theSupplier = CompressedIntsIndexedSupplier.fromIntBuffer(
        IntBuffer.wrap(vals), chunkSize, ByteOrder.nativeOrder(), compressionStrategy
    );
    theSupplier.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(theSupplier.getSerializedSize(), bytes.length);

    supplier = CompressedIntsIndexedSupplier.fromByteBuffer(ByteBuffer.wrap(bytes), ByteOrder.nativeOrder(), null);
    indexed = supplier.get();
  }

  private void setupLargeChunks(final int chunkSize, final int totalSize) throws IOException
  {
    vals = new int[totalSize];
    Random rand = new Random(0);
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt();
    }

    makeWithSerde(chunkSize);
  }

  @Test
  public void testSanity() throws Exception
  {
    setupSimple(5);

    Assert.assertEquals(4, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();

    // test powers of 2
    setupSimple(4);
    Assert.assertEquals(4, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();

    setupSimple(32);
    Assert.assertEquals(1, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();
  }

  @Test
  public void testLargeChunks() throws Exception
  {
    final int maxChunkSize = CompressedPools.BUFFER_SIZE / Longs.BYTES;

    setupLargeChunks(maxChunkSize, 10 * maxChunkSize);
    Assert.assertEquals(10, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();

    setupLargeChunks(maxChunkSize, 10 * maxChunkSize + 1);
    Assert.assertEquals(11, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();

    setupLargeChunks(maxChunkSize - 1, 10 * (maxChunkSize - 1) + 1);
    Assert.assertEquals(11, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChunkTooBig() throws Exception
  {
    final int maxChunkSize = CompressedPools.BUFFER_SIZE / Ints.BYTES;
    setupLargeChunks(maxChunkSize + 1, 10 * (maxChunkSize + 1));
  }

  @Test
  public void testBulkFill() throws Exception
  {
    setupSimple(5);

    tryFill(0, 15);
    tryFill(3, 6);
    tryFill(7, 7);
    tryFill(7, 9);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBulkFillTooMuch() throws Exception
  {
    setupSimple(5);
    tryFill(7, 10);
  }

  @Test
  public void testSanityWithSerde() throws Exception
  {
    setupSimpleWithSerde(5);

    Assert.assertEquals(4, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();
  }

  @Test
  public void testBulkFillWithSerde() throws Exception
  {
    setupSimpleWithSerde(5);

    tryFill(0, 15);
    tryFill(3, 6);
    tryFill(7, 7);
    tryFill(7, 9);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBulkFillTooMuchWithSerde() throws Exception
  {
    setupSimpleWithSerde(5);
    tryFill(7, 10);
  }

  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  @Test
  public void testConcurrentThreadReads() throws Exception
  {
    setupSimple(5);

    final AtomicReference<String> reason = new AtomicReference<String>("none");

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
                reason.set(StringUtils.format("Thread1[%d]: %d != %d", j, val, indexedVal));
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
                  reason.set(StringUtils.format("Thread2[%d]: %d != %d", j, val, indexedVal));
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

  private void tryFill(final int startIndex, final int size)
  {
    int[] filled = new int[size];
    indexed.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i]);
    }
  }

  private void assertIndexMatchesVals()
  {
    Assert.assertEquals(vals.length, indexed.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0; i < indexed.size(); ++i) {
      Assert.assertEquals(vals[i], indexed.get(i), 0.0);
      indices[i] = i;
    }

    // random access, limited to 1000 elements for large lists (every element would take too long)
    IntArrays.shuffle(indices, ThreadLocalRandom.current());
    final int limit = Math.min(indexed.size(), 1000);
    for (int i = 0; i < limit; ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], indexed.get(k), 0.0);
    }
  }
}
