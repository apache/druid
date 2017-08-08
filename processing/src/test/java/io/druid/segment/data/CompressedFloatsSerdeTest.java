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

import com.google.common.base.Supplier;
import com.google.common.io.ByteSink;
import com.google.common.primitives.Floats;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class CompressedFloatsSerdeTest
{
  @Parameterized.Parameters(name = "{0} {1} {2}")
  public static Iterable<Object[]> compressionStrategies()
  {
    List<Object[]> data = new ArrayList<>();
    for (CompressedObjectStrategy.CompressionStrategy strategy : CompressedObjectStrategy.CompressionStrategy.values()) {
      data.add(new Object[]{strategy, ByteOrder.BIG_ENDIAN});
      data.add(new Object[]{strategy, ByteOrder.LITTLE_ENDIAN});
    }
    return data;
  }

  private static final double DELTA = 0.00001;

  protected final CompressedObjectStrategy.CompressionStrategy compressionStrategy;
  protected final ByteOrder order;

  private final float values0[] = {};
  private final float values1[] = {0f, 1f, 1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 1f, 1f};
  private final float values2[] = {13.2f, 6.1f, 0.001f, 123f, 12572f, 123.1f, 784.4f, 6892.8634f, 8.341111f};
  private final float values3[] = {0.001f, 0.001f, 0.001f, 0.001f, 0.001f, 100f, 100f, 100f, 100f, 100f};
  private final float values4[] = {0f, 0f, 0f, 0f, 0.01f, 0f, 0f, 0f, 21.22f, 0f, 0f, 0f, 0f, 0f, 0f};
  private final float values5[] = {123.16f, 1.12f, 62.00f, 462.12f, 517.71f, 56.54f, 971.32f, 824.22f, 472.12f, 625.26f};
  private final float values6[] = {1000000f, 1000001f, 1000002f, 1000003f, 1000004f, 1000005f, 1000006f, 1000007f, 1000008f};
  private final float values7[] = {
      Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 12378.5734f, -12718243.7496f, -93653653.1f, 12743153.385534f,
      21431.414538f, 65487435436632.123f, -43734526234564.65f
  };

  public CompressedFloatsSerdeTest(
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      ByteOrder order
  )
  {
    this.compressionStrategy = compressionStrategy;
    this.order = order;
  }

  @Test
  public void testValueSerde() throws Exception
  {
    testWithValues(values0);
    testWithValues(values1);
    testWithValues(values2);
    testWithValues(values3);
    testWithValues(values4);
    testWithValues(values5);
    testWithValues(values6);
    testWithValues(values7);
  }

  @Test
  public void testChunkSerde() throws Exception
  {
    float chunk[] = new float[10000];
    for (int i = 0; i < 10000; i++) {
      chunk[i] = i;
    }
    testWithValues(chunk);
  }

  public void testWithValues(float[] values) throws Exception
  {
    FloatSupplierSerializer serializer = CompressionFactory.getFloatSerializer(new IOPeonForTesting(), "test", order, compressionStrategy
    );
    serializer.open();

    for (float value : values) {
      serializer.add(value);
    }
    Assert.assertEquals(values.length, serializer.size());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.closeAndConsolidate(
        new ByteSink()
        {
          @Override
          public OutputStream openStream() throws IOException
          {
            return baos;
          }
        }
    );
    Assert.assertEquals(baos.size(), serializer.getSerializedSize());
    CompressedFloatsIndexedSupplier supplier = CompressedFloatsIndexedSupplier
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order, null);
    IndexedFloats floats = supplier.get();

    assertIndexMatchesVals(floats, values);
    for (int i = 0; i < 10; i++) {
      int a = (int) (Math.random() * values.length);
      int b = (int) (Math.random() * values.length);
      int start = a < b ? a : b;
      int end = a < b ? b : a;
      tryFill(floats, values, start, end - start);
    }
    testSupplierSerde(supplier, values);
    testConcurrentThreadReads(supplier, floats, values);

    floats.close();
  }

  private void tryFill(IndexedFloats indexed, float[] vals, final int startIndex, final int size)
  {
    float[] filled = new float[size];
    indexed.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i], DELTA);
    }
  }

  private void assertIndexMatchesVals(IndexedFloats indexed, float[] vals)
  {
    Assert.assertEquals(vals.length, indexed.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0; i < indexed.size(); ++i) {
      Assert.assertEquals(vals[i], indexed.get(i), DELTA);
      indices[i] = i;
    }

    // random access, limited to 1000 elements for large lists (every element would take too long)
    IntArrays.shuffle(indices, ThreadLocalRandom.current());
    final int limit = Math.min(indexed.size(), 1000);
    for (int i = 0; i < limit; ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], indexed.get(k), DELTA);
    }
  }

  private void testSupplierSerde(CompressedFloatsIndexedSupplier supplier, float[] vals) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    supplier.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(supplier.getSerializedSize(), bytes.length);
    CompressedFloatsIndexedSupplier anotherSupplier = CompressedFloatsIndexedSupplier.fromByteBuffer(
        ByteBuffer.wrap(bytes), order, null
    );
    IndexedFloats indexed = anotherSupplier.get();
    assertIndexMatchesVals(indexed, vals);
  }

  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  private void testConcurrentThreadReads(
      final Supplier<IndexedFloats> supplier,
      final IndexedFloats indexed, final float[] vals
  ) throws Exception
  {
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
                reason.set(StringUtils.format("Thread1[%d]: %f != %f", j, val, indexedVal));
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
                  reason.set(StringUtils.format("Thread2[%d]: %f != %f", j, val, indexedVal));
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
}
