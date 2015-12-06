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

import com.google.common.io.Closeables;
import com.google.common.primitives.Floats;
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
import java.nio.FloatBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class CompressedFloatsIndexedSupplierTest extends CompressionStrategyTest
{
  public CompressedFloatsIndexedSupplierTest(CompressedObjectStrategy.CompressionStrategy compressionStrategy)
  {
    super(compressionStrategy);
  }

  private IndexedFloats indexed;
  private CompressedFloatsIndexedSupplier supplier;
  private float[] vals;

  @Before
  public void setUp() throws Exception
  {
    Closeables.close(indexed, false);
    indexed = null;
    supplier = null;
    vals = null;
  }

  @After
  public void tearDown() throws Exception
  {
    Closeables.close(indexed, false);
  }

  private void setupSimple(final int chunkSize)
  {
    CloseQuietly.close(indexed);

    vals = new float[]{
        0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 0.10f, 0.11f, 0.12f, 0.13f, 0.14f, 0.15f, 0.16f
    };

    supplier = CompressedFloatsIndexedSupplier.fromFloatBuffer(
        FloatBuffer.wrap(vals),
        chunkSize,
        ByteOrder.nativeOrder(),
        compressionStrategy
    );

    indexed = supplier.get();
  }

  private void setupSimpleWithSerde(final int chunkSize) throws IOException
  {
    vals = new float[]{
        0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 0.10f, 0.11f, 0.12f, 0.13f, 0.14f, 0.15f, 0.16f
    };

    makeWithSerde(chunkSize);
  }

  private void makeWithSerde(int chunkSize) throws IOException
  {
    CloseQuietly.close(indexed);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final CompressedFloatsIndexedSupplier theSupplier = CompressedFloatsIndexedSupplier.fromFloatBuffer(
        FloatBuffer.wrap(vals), chunkSize, ByteOrder.nativeOrder(), compressionStrategy
    );
    theSupplier.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(theSupplier.getSerializedSize(), bytes.length);

    supplier = CompressedFloatsIndexedSupplier.fromByteBuffer(ByteBuffer.wrap(bytes), ByteOrder.nativeOrder());
    indexed = supplier.get();
  }

  private void setupLargeChunks(final int chunkSize, final int totalSize) throws IOException
  {
    vals = new float[totalSize];
    Random rand = new Random(0);
    for(int i = 0; i < vals.length; ++i) {
      vals[i] = (float)rand.nextGaussian();
    }

    makeWithSerde(chunkSize);
  }

  @Test
  public void testSanity() throws Exception
  {
    setupSimple(5);
    Assert.assertEquals(4, supplier.getBaseFloatBuffers().size());
    assertIndexMatchesVals();

    // test powers of 2
    setupSimple(2);
    Assert.assertEquals(9, supplier.getBaseFloatBuffers().size());
    assertIndexMatchesVals();
  }

  @Test
  public void testLargeChunks() throws Exception
  {
    final int maxChunkSize = CompressedPools.BUFFER_SIZE / Floats.BYTES;

    setupLargeChunks(maxChunkSize, 10 * maxChunkSize);
    Assert.assertEquals(10, supplier.getBaseFloatBuffers().size());
    assertIndexMatchesVals();

    setupLargeChunks(maxChunkSize, 10 * maxChunkSize + 1);
    Assert.assertEquals(11, supplier.getBaseFloatBuffers().size());
    assertIndexMatchesVals();

    setupLargeChunks(maxChunkSize - 1, 10 * (maxChunkSize - 1) + 1);
    Assert.assertEquals(11, supplier.getBaseFloatBuffers().size());
    assertIndexMatchesVals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChunkTooBig() throws Exception
  {
    final int maxChunkSize = CompressedPools.BUFFER_SIZE / Floats.BYTES;
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
    tryFill(7, 11);
  }

  @Test
  public void testSanityWithSerde() throws Exception
  {
    setupSimpleWithSerde(5);

    Assert.assertEquals(4, supplier.getBaseFloatBuffers().size());

    assertIndexMatchesVals();

    // test powers of 2
    setupSimpleWithSerde(2);

    Assert.assertEquals(9, supplier.getBaseFloatBuffers().size());

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
    tryFill(7, 11);
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
              final float val = vals[j];
              final float indexedVal = indexed.get(j);
              if (Floats.compare(val, indexedVal) != 0) {
                failureHappened.set(true);
                reason.set(String.format("Thread1[%d]: %f != %f", j, val, indexedVal));
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

    final IndexedFloats indexed2 = supplier.get();
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
                final float val = vals[j];
                final float indexedVal = indexed2.get(j);
                if (Floats.compare(val, indexedVal) != 0) {
                  failureHappened.set(true);
                  reason.set(String.format("Thread2[%d]: %f != %f", j, val, indexedVal));
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
      indexed2.close();
    }

    if (failureHappened.get()) {
      Assert.fail("Failure happened.  Reason: " + reason.get());
    }
  }

  private void tryFill(final int startIndex, final int size)
  {
    float[] filled = new float[size];
    indexed.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i], 0.0);
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

    Collections.shuffle(Arrays.asList(indices));
    // random access
    for (int i = 0; i < indexed.size(); ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], indexed.get(k), 0.0);
    }
  }
}
