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

package org.apache.druid.segment.data;

import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.CompressedPools;
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

public class CompressedColumnarIntsSupplierTest extends CompressionStrategyTest
{
  public CompressedColumnarIntsSupplierTest(CompressionStrategy compressionStrategy)
  {
    super(compressionStrategy);
  }

  private Closer closer;
  private ColumnarInts columnarInts;
  private CompressedColumnarIntsSupplier supplier;
  private int[] vals;

  @Before
  public void setUp()
  {
    closer = Closer.create();
    CloseQuietly.close(columnarInts);
    columnarInts = null;
    supplier = null;
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
    CloseQuietly.close(columnarInts);
  }

  private void setupSimple(final int chunkSize)
  {
    CloseQuietly.close(columnarInts);

    vals = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16};

    supplier = CompressedColumnarIntsSupplier.fromIntBuffer(
        IntBuffer.wrap(vals),
        chunkSize,
        ByteOrder.nativeOrder(),
        compressionStrategy,
        closer
    );

    columnarInts = supplier.get();
  }

  private void setupSimpleWithSerde(final int chunkSize) throws IOException
  {
    vals = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16};

    makeWithSerde(chunkSize);
  }

  private void makeWithSerde(final int chunkSize) throws IOException
  {
    CloseQuietly.close(columnarInts);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final CompressedColumnarIntsSupplier theSupplier = CompressedColumnarIntsSupplier.fromIntBuffer(
        IntBuffer.wrap(vals), chunkSize, ByteOrder.nativeOrder(), compressionStrategy, closer
    );
    theSupplier.writeTo(Channels.newChannel(baos), null);

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(theSupplier.getSerializedSize(), bytes.length);

    supplier = CompressedColumnarIntsSupplier.fromByteBuffer(ByteBuffer.wrap(bytes), ByteOrder.nativeOrder());
    columnarInts = supplier.get();
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
  public void testSanity()
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
    final int maxChunkSize = CompressedPools.BUFFER_SIZE / Long.BYTES;

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
    final int maxChunkSize = CompressedPools.BUFFER_SIZE / Integer.BYTES;
    setupLargeChunks(maxChunkSize + 1, 10 * (maxChunkSize + 1));
  }

  @Test
  public void testSanityWithSerde() throws Exception
  {
    setupSimpleWithSerde(5);

    Assert.assertEquals(4, supplier.getBaseIntBuffers().size());
    assertIndexMatchesVals();
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
            for (int j = 0, size = columnarInts.size(); j < size; ++j) {
              final long val = vals[j];
              final long indexedVal = columnarInts.get(j);
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

    final ColumnarInts columnarInts2 = supplier.get();
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
              for (int j = columnarInts2.size() - 1; j >= 0; --j) {
                final long val = vals[j];
                final long indexedVal = columnarInts2.get(j);
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
      CloseQuietly.close(columnarInts2);
    }

    if (failureHappened.get()) {
      Assert.fail("Failure happened.  Reason: " + reason.get());
    }
  }

  private void assertIndexMatchesVals()
  {
    Assert.assertEquals(vals.length, columnarInts.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0, size = columnarInts.size(); i < size; ++i) {
      Assert.assertEquals(vals[i], columnarInts.get(i), 0.0);
      indices[i] = i;
    }

    // random access, limited to 1000 elements for large lists (every element would take too long)
    IntArrays.shuffle(indices, ThreadLocalRandom.current());
    final int limit = Math.min(columnarInts.size(), 1000);
    for (int i = 0; i < limit; ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], columnarInts.get(k), 0.0);
    }
  }
}
