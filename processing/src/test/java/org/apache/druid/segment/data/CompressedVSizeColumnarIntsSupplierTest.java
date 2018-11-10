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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.CompressedPools;
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
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class CompressedVSizeColumnarIntsSupplierTest extends CompressionStrategyTest
{
  @Parameterized.Parameters(name = "{index}: compression={0}, byteOrder={1}")
  public static Iterable<Object[]> compressionStrategies()
  {
    final Iterable<CompressionStrategy> compressionStrategies = Iterables.transform(
        CompressionStrategyTest.compressionStrategies(),
        (Object[] input) -> (CompressionStrategy) input[0]
    );

    Set<List<Object>> combinations = Sets.cartesianProduct(
        Sets.newHashSet(compressionStrategies),
        Sets.newHashSet(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
    );

    return Iterables.transform(
        combinations,
        (Function<List, Object[]>) input -> new Object[]{input.get(0), input.get(1)}
    );
  }

  private static final int[] MAX_VALUES = new int[] {0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};

  public CompressedVSizeColumnarIntsSupplierTest(CompressionStrategy compressionStrategy, ByteOrder byteOrder)
  {
    super(compressionStrategy);
    this.byteOrder = byteOrder;
  }

  private Closer closer;
  private ColumnarInts columnarInts;
  private CompressedVSizeColumnarIntsSupplier supplier;
  private int[] vals;
  private final ByteOrder byteOrder;


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
    CloseQuietly.close(columnarInts);
    closer.close();
  }

  private void setupSimple(final int chunkSize)
  {
    CloseQuietly.close(columnarInts);

    vals = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16};

    supplier = CompressedVSizeColumnarIntsSupplier.fromList(
        IntArrayList.wrap(vals),
        Ints.max(vals),
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
    final CompressedVSizeColumnarIntsSupplier theSupplier = CompressedVSizeColumnarIntsSupplier.fromList(
        IntArrayList.wrap(vals), Ints.max(vals), chunkSize, byteOrder, compressionStrategy, closer
    );
    theSupplier.writeTo(Channels.newChannel(baos), null);

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(theSupplier.getSerializedSize(), bytes.length);

    supplier = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(ByteBuffer.wrap(bytes), byteOrder);
    columnarInts = supplier.get();
  }

  private void setupLargeChunks(final int chunkSize, final int totalSize, final int maxValue) throws IOException
  {
    vals = new int[totalSize];
    Random rand = new Random(0);
    for (int i = 0; i < vals.length; ++i) {
      // VSizeColumnarMultiInts only allows positive values
      vals[i] = rand.nextInt(maxValue);
    }

    makeWithSerde(chunkSize);
  }

  @Test
  public void testSanity()
  {
    setupSimple(2);
    Assert.assertEquals(8, supplier.getBaseBuffers().size());
    assertIndexMatchesVals();

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
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);

      setupLargeChunks(maxChunkSize, 10 * maxChunkSize, maxValue);
      Assert.assertEquals(10, supplier.getBaseBuffers().size());
      assertIndexMatchesVals();

      setupLargeChunks(maxChunkSize, 10 * maxChunkSize + 1, maxValue);
      Assert.assertEquals(11, supplier.getBaseBuffers().size());
      assertIndexMatchesVals();

      setupLargeChunks(1, 0xFFFF, maxValue);
      Assert.assertEquals(0xFFFF, supplier.getBaseBuffers().size());
      assertIndexMatchesVals();

      setupLargeChunks(maxChunkSize / 2, 10 * (maxChunkSize / 2) + 1, maxValue);
      Assert.assertEquals(11, supplier.getBaseBuffers().size());
      assertIndexMatchesVals();
    }
  }

  @Test
  public void testChunkTooBig() throws Exception
  {
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      try {
        setupLargeChunks(maxChunkSize + 1, 10 * (maxChunkSize + 1), maxValue);
        Assert.fail();
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("chunk too big for maxValue " + maxValue, true);
      }
    }
  }

  @Test
  public void testmaxIntsInBuffer()
  {
    Assert.assertEquals(CompressedPools.BUFFER_SIZE, CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForBytes(1));
    Assert.assertEquals(CompressedPools.BUFFER_SIZE / 2, CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForBytes(2));
    Assert.assertEquals(CompressedPools.BUFFER_SIZE / 4, CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForBytes(4));

    Assert.assertEquals(CompressedPools.BUFFER_SIZE, 0x10000); // nearest power of 2 is 2^14
    Assert.assertEquals(1 << 14, CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForBytes(3));
  }

  @Test
  public void testSanityWithSerde() throws Exception
  {
    setupSimpleWithSerde(4);

    Assert.assertEquals(4, supplier.getBaseBuffers().size());
    assertIndexMatchesVals();

    setupSimpleWithSerde(2);

    Assert.assertEquals(8, supplier.getBaseBuffers().size());
    assertIndexMatchesVals();
  }


  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  @Test
  public void testConcurrentThreadReads() throws Exception
  {
    setupSimple(4);

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

    // sequential access of every element
    int[] indices = new int[vals.length];
    for (int i = 0, size = columnarInts.size(); i < size; ++i) {
      final int expected = vals[i];
      final int actual = columnarInts.get(i);
      Assert.assertEquals(expected, actual);
      indices[i] = i;
    }

    // random access, limited to 1000 elements for large lists (every element would take too long)
    IntArrays.shuffle(indices, ThreadLocalRandom.current());
    final int limit = Math.min(columnarInts.size(), 1000);
    for (int i = 0; i < limit; ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], columnarInts.get(k));
    }
  }
}
