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
import com.google.common.primitives.Longs;
import io.druid.java.util.common.guava.CloseQuietly;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class CompressedLongsSerdeTest
{
  @Parameterized.Parameters(name = "{0} {1} {2}")
  public static Iterable<Object[]> compressionStrategies()
  {
    List<Object[]> data = new ArrayList<>();
    for (CompressionFactory.LongEncodingStrategy encodingStrategy: CompressionFactory.LongEncodingStrategy.values()) {
      for (CompressedObjectStrategy.CompressionStrategy strategy : CompressedObjectStrategy.CompressionStrategy.values()) {
        data.add(new Object[]{encodingStrategy, strategy, ByteOrder.BIG_ENDIAN});
        data.add(new Object[]{encodingStrategy, strategy, ByteOrder.LITTLE_ENDIAN});
      }
    }
    return data;
  }

  protected final CompressionFactory.LongEncodingStrategy encodingStrategy;
  protected final CompressedObjectStrategy.CompressionStrategy compressionStrategy;
  protected final ByteOrder order;

  private final long values0[] = {};
  private final long values1[] = {0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1};
  private final long values2[] = {12, 5, 2, 9, 3, 2, 5, 1, 0, 6, 13, 10, 15};
  private final long values3[] = {1, 1, 1, 1, 1, 11, 11, 11, 11};
  private final long values4[] = {200, 200, 200, 401, 200, 301, 200, 200, 200, 404, 200, 200, 200, 200};
  private final long values5[] = {123, 632, 12, 39, 536, 0, 1023, 52, 777, 526, 214, 562, 823, 346};
  private final long values6[] = {1000000, 1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007, 1000008};
  private final long values7[] = {
      Long.MAX_VALUE, Long.MIN_VALUE, 12378, -12718243, -1236213, 12743153, 21364375452L,
      65487435436632L, -43734526234564L
  };
  private final long values8[] = {Long.MAX_VALUE, 0, 321, 15248425, 13523212136L, 63822, 3426, 96};

  // built test value with enough unique values to not use table encoding for auto strategy
  private static long[] addUniques(long[] val) {
    long[] ret = new long[val.length + CompressionFactory.MAX_TABLE_SIZE];
    for (int i = 0; i < CompressionFactory.MAX_TABLE_SIZE; i++) {
      ret[i] = i;
    }
    System.arraycopy(val, 0, ret, 256, val.length);
    return ret;
  }

  public CompressedLongsSerdeTest(
      CompressionFactory.LongEncodingStrategy encodingStrategy,
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      ByteOrder order
  )
  {
    this.encodingStrategy = encodingStrategy;
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
    testWithValues(values8);
  }

  @Test
  public void testChunkSerde() throws Exception
  {
    long chunk[] = new long[10000];
    for (int i = 0; i < 10000; i++) {
      chunk[i] = i;
    }
    testWithValues(chunk);
  }

  public void testWithValues(long[] values) throws Exception
  {
    testValues(values);
    testValues(addUniques(values));
  }

  public void testValues(long[] values) throws Exception
  {
    LongSupplierSerializer serializer = CompressionFactory.getLongSerializer(new IOPeonForTesting(), "test", order,
                                                                             encodingStrategy, compressionStrategy
    );
    serializer.open();

    for (long value : values) {
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
    CompressedLongsIndexedSupplier supplier = CompressedLongsIndexedSupplier
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order);
    IndexedLongs longs = supplier.get();

    assertIndexMatchesVals(longs, values);
    for (int i = 0; i < 10; i++) {
      int a = (int) (Math.random() * values.length);
      int b = (int) (Math.random() * values.length);
      int start = a < b ? a : b;
      int end = a < b ? b : a;
      tryFill(longs, values, start, end - start);
    }
    testSupplierSerde(supplier, values);
    testConcurrentThreadReads(supplier, longs, values);

    longs.close();
  }

  private void tryFill(IndexedLongs indexed, long[] vals, final int startIndex, final int size)
  {
    long[] filled = new long[size];
    indexed.fill(startIndex, filled);

    for (int i = startIndex; i < filled.length; i++) {
      Assert.assertEquals(vals[i + startIndex], filled[i]);
    }
  }

  private void assertIndexMatchesVals(IndexedLongs indexed, long[] vals)
  {
    Assert.assertEquals(vals.length, indexed.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0; i < indexed.size(); ++i) {
      Assert.assertEquals(vals[i], indexed.get(i));
      indices[i] = i;
    }

    Collections.shuffle(Arrays.asList(indices));
    // random access
    for (int i = 0; i < indexed.size(); ++i) {
      int k = indices[i];
      Assert.assertEquals(vals[k], indexed.get(k));
    }
  }

  private void testSupplierSerde(CompressedLongsIndexedSupplier supplier, long[] vals) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    supplier.writeToChannel(Channels.newChannel(baos));

    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(supplier.getSerializedSize(), bytes.length);
    CompressedLongsIndexedSupplier anotherSupplier = CompressedLongsIndexedSupplier.fromByteBuffer(
        ByteBuffer.wrap(bytes), order
    );
    IndexedLongs indexed = anotherSupplier.get();
    assertIndexMatchesVals(indexed, vals);
  }

  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  private void testConcurrentThreadReads(
      final Supplier<IndexedLongs> supplier,
      final IndexedLongs indexed, final long[] vals
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

    final IndexedLongs indexed2 = supplier.get();
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
}
