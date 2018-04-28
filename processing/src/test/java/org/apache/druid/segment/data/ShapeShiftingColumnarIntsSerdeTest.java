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

import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.codecs.CompressedFormEncoder;
import org.apache.druid.segment.data.codecs.ints.BytePackedIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.IntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.LemireIntFormEncoder;
import org.apache.druid.segment.data.codecs.ints.RunLengthBytePackedIntFormEncoder;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
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
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class ShapeShiftingColumnarIntsSerdeTest
{

  @Parameterized.Parameters(name = "{index}: blockSize={0}, optimizationTarget={1}, byteOrder={2}, encoderSet={3}")
  public static List<Object[]> blockSizesOptimizationTargetsAndByteOrders()
  {
    Set<List<Object>> combinations = Sets.cartesianProduct(
        Sets.newHashSet(IndexSpec.ShapeShiftBlockSize.values()),
        Sets.newHashSet(IndexSpec.ShapeShiftOptimizationTarget.values()),
        Sets.newHashSet(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN),
        Sets.newHashSet("bytepack", "rle-bytepack", "fastpfor", "lz4-bytepack", "lz4-rle-bytepack", "lz4", "default")
    );

    return combinations.stream()
                       .map(input -> new Object[]{input.get(0), input.get(1), input.get(2), input.get(3)})
                       .collect(Collectors.toList());
  }

  private final IndexSpec.ShapeShiftBlockSize blockSize;
  private final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget;
  private final ByteOrder byteOrder;
  private final String encoderSet;

  private Closer closer;
  private ColumnarInts columnarInts;
  private ShapeShiftingColumnarIntsSupplier supplier;
  private int[] vals;

  public ShapeShiftingColumnarIntsSerdeTest(
      IndexSpec.ShapeShiftBlockSize blockSize,
      IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      ByteOrder byteOrder,
      String encoderSet
  )
  {
    this.blockSize = blockSize;
    this.optimizationTarget = optimizationTarget;
    this.byteOrder = byteOrder;
    this.encoderSet = encoderSet;
  }

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
    columnarInts.close();
    closer.close();
  }
  
  @Test
  public void testFidelity() throws Exception
  {
    int intChunkSize = 1 << (blockSize.getLogBlockSize() - 2);

    seedAndEncodeData(10 * intChunkSize);

    assertIndexMatchesVals();
  }


  // cargo cult test, this guy is everywhere so why not here too
  @Test
  public void testConcurrentThreadReads() throws Exception
  {
    int intChunkSize = 1 << (blockSize.getLogBlockSize() - 2);
    seedAndEncodeData(3 * intChunkSize);

    final AtomicReference<String> reason = new AtomicReference<>("none");

    final int numRuns = 1000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch stopLatch = new CountDownLatch(2);
    final AtomicBoolean failureHappened = new AtomicBoolean(false);
    new Thread(() -> {
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
    }).start();

    final ColumnarInts columnarInts2 = supplier.get();
    try {
      new Thread(() -> {
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

  private void serializeAndGetSupplier() throws IOException
  {
    SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();

    final SegmentWriteOutMedium segmentWriteOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    IntFormEncoder[] encoders = ShapeShiftingColumnarIntsSerializer.getDefaultIntFormEncoders(
      blockSize,
      CompressionStrategy.LZ4,
      writeOutMedium.getCloser(),
      byteOrder
    );

    List<IntFormEncoder> encoderList = Arrays.asList(encoders);

    // todo: probably a better way to do this...
    switch (encoderSet) {
      case "bytepack":
        encoders = encoderList.stream()
                              .filter(e -> e instanceof BytePackedIntFormEncoder)
                              .toArray(IntFormEncoder[]::new);
        break;
      case "rle-bytepack":
        encoders = encoderList.stream()
                              .filter(e -> e instanceof RunLengthBytePackedIntFormEncoder)
                              .toArray(IntFormEncoder[]::new);
        break;
      case "fastpfor":
        encoders = encoderList.stream()
                              .filter(e -> e instanceof LemireIntFormEncoder)
                              .toArray(IntFormEncoder[]::new);
        break;
      case "lz4-bytepack":
        encoders =
            encoderList.stream()
                       .filter(e -> e instanceof CompressedFormEncoder &&
                                    ((CompressedFormEncoder) e).getInnerEncoder() instanceof BytePackedIntFormEncoder
                       ).toArray(IntFormEncoder[]::new);
        break;
      case "lz4-rle-bytepack":
        encoders =
            encoderList.stream()
                       .filter(e -> e instanceof CompressedFormEncoder &&
                                    ((CompressedFormEncoder) e).getInnerEncoder() instanceof RunLengthBytePackedIntFormEncoder
                       ).toArray(IntFormEncoder[]::new);
        break;
      case "lz4":
        encoders =
            encoderList.stream().filter(e -> e instanceof CompressedFormEncoder).toArray(IntFormEncoder[]::new);
        break;
    }

    ShapeShiftingColumnarIntsSerializer serializer = new ShapeShiftingColumnarIntsSerializer(
        segmentWriteOutMedium,
        encoders,
        optimizationTarget,
        blockSize,
        byteOrder
    );
    serializer.open();
    for (int val : vals) {
      serializer.addValue(val);
    }
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.writeTo(Channels.newChannel(baos), null);

    writeOutMedium.close();
    final byte[] bytes = baos.toByteArray();
    Assert.assertEquals(serializer.getSerializedSize(), bytes.length);

    supplier = ShapeShiftingColumnarIntsSupplier.fromByteBuffer(ByteBuffer.wrap(bytes), byteOrder);
    columnarInts = supplier.get();
  }


  private void seedAndEncodeData(final int totalSize) throws IOException
  {
    int intChunkSize = 1 << (blockSize.getLogBlockSize() - 2);

    vals = new int[totalSize];
    Random rand = new Random(0);
    boolean isZero = false;
    boolean isConstant = false;
    int constant = 0;

    for (int i = 0; i < vals.length; ++i) {
      // occasionally write a zero or constant block for funsies
      if (i != 0 && (i % intChunkSize == 0)) {
        isZero = false;
        isConstant = false;
        int rando = rand.nextInt(3);
        switch (rando) {
          case 0:
            isZero = true;
            break;
          case 1:
            isConstant = true;
            constant = rand.nextInt((1 << 31) - 1);
            break;

        }
      }

      if (isZero) {
        vals[i] = 0;
      } else if (isConstant) {
        vals[i] = constant;
      } else {
        vals[i] = rand.nextInt((1 << 31) - 1);
      }
    }

    serializeAndGetSupplier();
  }


  // todo: copy pasta, maybe should share these validation methods between tests
  private void assertIndexMatchesVals()
  {
    Assert.assertEquals(vals.length, columnarInts.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0, size = columnarInts.size(); i < size; i++) {
      Assert.assertEquals("row mismatch at " + i, vals[i], columnarInts.get(i), 0.0);
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
