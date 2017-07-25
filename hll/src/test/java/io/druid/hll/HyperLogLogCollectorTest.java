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

package io.druid.hll;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 */
public class HyperLogLogCollectorTest
{

  private final HashFunction fn = Hashing.murmur3_128();

  @Test
  public void testFolding() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();

      folded.fold(oneHalf);
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf);
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }


  /**
   * This is a very long running test, disabled by default.
   * It is meant to catch issues when combining a large numer of HLL objects.
   *
   * It compares adding all the values to one HLL vs.
   * splitting up values into HLLs of 100 values each, and folding those HLLs into a single main HLL.
   *
   * When reaching very large cardinalities (>> 50,000,000), offsets are mismatched between the main HLL and the ones
   * with 100 values, requiring  a floating max as described in
   * http://druid.io/blog/2014/02/18/hyperloglog-optimizations-for-real-world-systems.html
   */
  @Ignore
  @Test
  public void testHighCardinalityRollingFold() throws Exception
  {
    final HyperLogLogCollector rolling = HyperLogLogCollector.makeLatestCollector();
    final HyperLogLogCollector simple = HyperLogLogCollector.makeLatestCollector();

    MessageDigest md = MessageDigest.getInstance("SHA-1");
    HyperLogLogCollector tmp = HyperLogLogCollector.makeLatestCollector();

    int count;
    for (count = 0; count < 100_000_000; ++count) {
      md.update(StringUtils.toUtf8(Integer.toString(count)));

      byte[] hashed = fn.hashBytes(md.digest()).asBytes();

      tmp.add(hashed);
      simple.add(hashed);

      if (count % 100 == 0) {
        rolling.fold(tmp);
        tmp = HyperLogLogCollector.makeLatestCollector();
      }
    }

    int n = count;

    System.out.println("True cardinality " + n);
    System.out.println("Rolling buffer cardinality " + rolling.estimateCardinality());
    System.out.println("Simple  buffer cardinality " + simple.estimateCardinality());
    System.out.println(
        StringUtils.format(
            "Rolling cardinality estimate off by %4.1f%%",
            100 * (1 - rolling.estimateCardinality() / n)
        )
    );

    Assert.assertEquals(n, simple.estimateCardinality(), n * 0.05);
    Assert.assertEquals(n, rolling.estimateCardinality(), n * 0.05);
  }

  @Ignore
  @Test
  public void testHighCardinalityRollingFold2() throws Exception
  {
    final HyperLogLogCollector rolling = HyperLogLogCollector.makeLatestCollector();
    int count;
    long start = System.currentTimeMillis();

    for (count = 0; count < 50_000_000; ++count) {
      HyperLogLogCollector theCollector = HyperLogLogCollector.makeLatestCollector();
      theCollector.add(fn.hashLong(count).asBytes());
      rolling.fold(theCollector);
    }
    System.out.printf(
        Locale.ENGLISH,
        "testHighCardinalityRollingFold2 took %d ms%n",
        System.currentTimeMillis() - start
    );

    int n = count;

    System.out.println("True cardinality " + n);
    System.out.println("Rolling buffer cardinality " + rolling.estimateCardinality());
    System.out.println(
        StringUtils.format(
            "Rolling cardinality estimate off by %4.1f%%",
            100 * (1 - rolling.estimateCardinality() / n)
        )
    );

    Assert.assertEquals(n, rolling.estimateCardinality(), n * 0.05);
  }

  @Test
  public void testFoldingByteBuffers() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();

      folded.fold(oneHalf.toByteBuffer());
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf.toByteBuffer());
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }

  @Test
  public void testFoldingReadOnlyByteBuffers() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeCollector(
          ByteBuffer.wrap(HyperLogLogCollector.makeEmptyVersionedByteArray())
                    .asReadOnlyBuffer()
      );

      folded.fold(oneHalf.toByteBuffer());
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf.toByteBuffer());
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }

  @Test
  public void testFoldingReadOnlyByteBuffersWithArbitraryPosition() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeCollector(
          shiftedBuffer(
              ByteBuffer.wrap(HyperLogLogCollector.makeEmptyVersionedByteArray())
                        .asReadOnlyBuffer(),
              17
          )
      );

      folded.fold(oneHalf.toByteBuffer());
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf.toByteBuffer());
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }

  @Test
  public void testFoldWithDifferentOffsets1() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, (byte) 0x00, 0x11);
    ByteBuffer smallerOffset = makeCollectorBuffer(0, (byte) 0x20, 0x00);

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(biggerOffset);
    collector.fold(smallerOffset);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), 2047);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(smallerOffset);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), 2047);
    Assert.assertEquals(outBuffer.get(), 0);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }
  }

  @Test
  public void testBufferSwap() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, (byte) 0x00, 0x11);
    ByteBuffer smallerOffset = makeCollectorBuffer(0, (byte) 0x20, 0x00);

    ByteBuffer buffer = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buffer.duplicate());

    // make sure the original buffer gets modified
    collector.fold(biggerOffset);
    Assert.assertEquals(collector, HyperLogLogCollector.makeCollector(buffer.duplicate()));

    // make sure the original buffer gets modified
    collector.fold(smallerOffset);
    Assert.assertEquals(collector, HyperLogLogCollector.makeCollector(buffer.duplicate()));
  }

  @Test
  public void testFoldWithArbitraryInitialPositions() throws Exception
  {
    ByteBuffer biggerOffset = shiftedBuffer(makeCollectorBuffer(1, (byte) 0x00, 0x11), 10);
    ByteBuffer smallerOffset = shiftedBuffer(makeCollectorBuffer(0, (byte) 0x20, 0x00), 15);

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(biggerOffset);
    collector.fold(smallerOffset);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), 2047);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(smallerOffset);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), 2047);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }
  }

  protected ByteBuffer shiftedBuffer(ByteBuffer buf, int offset)
  {
    ByteBuffer shifted = ByteBuffer.allocate(buf.remaining() + offset);
    shifted.position(offset);
    shifted.put(buf);
    shifted.position(offset);
    return shifted;
  }

  @Test
  public void testFoldWithDifferentOffsets2() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, (byte) 0x01, 0x11);
    ByteBuffer smallerOffset = makeCollectorBuffer(0, (byte) 0x20, 0x00);

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(biggerOffset);
    collector.fold(smallerOffset);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertFalse(outBuffer.hasRemaining());

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(smallerOffset);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertFalse(outBuffer.hasRemaining());
  }

  @Test
  public void testFoldWithUpperNibbleTriggersOffsetChange() throws Exception
  {
    byte[] arr1 = new byte[HyperLogLogCollector.getLatestNumBytesForDenseStorage()];
    Arrays.fill(arr1, (byte) 0x11);
    ByteBuffer buffer1 = ByteBuffer.wrap(arr1);
    buffer1.put(0, HLLCV1.VERSION);
    buffer1.put(1, (byte) 0);
    buffer1.putShort(2, (short) (2047));
    buffer1.put(HLLCV1.HEADER_NUM_BYTES, (byte) 0x1);

    byte[] arr2 = new byte[HyperLogLogCollector.getLatestNumBytesForDenseStorage()];
    Arrays.fill(arr2, (byte) 0x11);
    ByteBuffer buffer2 = ByteBuffer.wrap(arr2);
    buffer2.put(0, HLLCV1.VERSION);
    buffer2.put(1, (byte) 0);
    buffer2.putShort(2, (short) (2048));

    HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buffer1);
    collector.fold(buffer2);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), HLLCV1.VERSION);
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), 0);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertFalse(outBuffer.hasRemaining());
  }

  @Test
  public void testSparseFoldWithDifferentOffsets1() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, new byte[]{0x11, 0x10}, 0x11);
    ByteBuffer sparse = HyperLogLogCollector.makeCollector(makeCollectorBuffer(0, new byte[]{0x00, 0x02}, 0x00))
                                            .toByteBuffer();

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(biggerOffset);
    collector.fold(sparse);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertEquals(outBuffer.get(), 0);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertFalse(outBuffer.hasRemaining());

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(sparse);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), collector.getVersion());
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertEquals(outBuffer.get(), 0);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertFalse(outBuffer.hasRemaining());
  }

  private ByteBuffer makeCollectorBuffer(int offset, byte initialBytes, int remainingBytes)
  {
    return makeCollectorBuffer(offset, new byte[]{initialBytes}, remainingBytes);
  }

  private ByteBuffer makeCollectorBuffer(int offset, byte[] initialBytes, int remainingBytes)
  {
    short numNonZero = 0;
    for (byte initialByte : initialBytes) {
      numNonZero += computeNumNonZero(initialByte);
    }

    final short numNonZeroInRemaining = computeNumNonZero((byte) remainingBytes);
    numNonZero += (short)((HyperLogLogCollector.NUM_BYTES_FOR_BUCKETS - initialBytes.length) * numNonZeroInRemaining);

    ByteBuffer biggerOffset = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    biggerOffset.put(HLLCV1.VERSION);
    biggerOffset.put((byte) offset);
    biggerOffset.putShort(numNonZero);
    biggerOffset.put((byte) 0);
    biggerOffset.putShort((short) 0);
    biggerOffset.put(initialBytes);
    while (biggerOffset.hasRemaining()) {
      biggerOffset.put((byte) remainingBytes);
    }
    biggerOffset.clear();
    return biggerOffset.asReadOnlyBuffer();
  }

  private short computeNumNonZero(byte theByte)
  {
    short retVal = 0;
    if ((theByte & 0x0f) > 0) {
      ++retVal;
    }
    if ((theByte & 0xf0) > 0) {
      ++retVal;
    }
    return retVal;
  }

  @Ignore @Test // This test can help when finding potential combinations that are weird, but it's non-deterministic
  public void testFoldingwithDifferentOffsets() throws Exception
  {
    // final Random random = new Random(37); // this seed will cause this test to fail because of slightly larger errors
    final Random random = new Random(0);
    for (int j = 0; j < 10; j++) {
      HyperLogLogCollector smallVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector bigVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector all = HyperLogLogCollector.makeLatestCollector();

      int numThings = 500000;
      for (int i = 0; i < numThings; i++) {
        byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();

        if (i < 1000) {
          smallVals.add(hashedVal);
        } else {
          bigVals.add(hashedVal);
        }
        all.add(hashedVal);
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();
      folded.fold(smallVals);
      folded.fold(bigVals);
      final double expected = all.estimateCardinality();
      Assert.assertEquals(expected, folded.estimateCardinality(), expected * 0.025);
      Assert.assertEquals(numThings, folded.estimateCardinality(), numThings * 0.05);
    }
  }

  @Ignore @Test
  public void testFoldingwithDifferentOffsets2() throws Exception
  {
    final Random random = new Random(0);
    MessageDigest md = MessageDigest.getInstance("SHA-1");

    for (int j = 0; j < 1; j++) {
      HyperLogLogCollector evenVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oddVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector all = HyperLogLogCollector.makeLatestCollector();

      int numThings = 500000;
      for (int i = 0; i < numThings; i++) {
        md.update(StringUtils.toUtf8(Integer.toString(random.nextInt())));
        byte[] hashedVal = fn.hashBytes(md.digest()).asBytes();

        if (i % 2 == 0) {
          evenVals.add(hashedVal);
        } else {
          oddVals.add(hashedVal);
        }
        all.add(hashedVal);
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();
      folded.fold(evenVals);
      folded.fold(oddVals);
      final double expected = all.estimateCardinality();
      Assert.assertEquals(expected, folded.estimateCardinality(), expected * 0.025);
      Assert.assertEquals(numThings, folded.estimateCardinality(), numThings * 0.05);
    }
  }

  @Test
  public void testEstimation() throws Exception
  {
    Random random = new Random(0L);

    final int[] valsToCheck = {10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000};
    final double[] expectedVals = {
        11.029647221949576, 21.108407720752034, 51.64575281885815, 100.42231726408892,
        981.8579991802412, 1943.1337257462792, 4946.192042635218, 9935.088157579434,
        20366.1486889433, 49433.56029693898, 100615.26273314281, 980831.624899156000,
        1982408.2608981386
    };

    int valsToCheckIndex = 0;
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    for (int i = 0; i < valsToCheck[valsToCheck.length - 1]; ++i) {
      collector.add(fn.hashLong(random.nextLong()).asBytes());
      if (i == valsToCheck[valsToCheckIndex]) {
        Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
        ++valsToCheckIndex;
      }
    }
    Assert.assertEquals(expectedVals.length, valsToCheckIndex + 1);
    Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
  }

  @Test
  public void testEstimationReadOnlyByteBuffers() throws Exception
  {
    Random random = new Random(0L);

    final int[] valsToCheck = {10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000};
    final double[] expectedVals = {
        11.029647221949576, 21.108407720752034, 51.64575281885815, 100.42231726408892,
        981.8579991802412, 1943.1337257462792, 4946.192042635218, 9935.088157579434,
        20366.1486889433, 49433.56029693898, 100615.26273314281, 980831.624899156000,
        1982408.2608981386
    };

    int valsToCheckIndex = 0;
    HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(
        ByteBuffer.allocateDirect(
            HyperLogLogCollector.getLatestNumBytesForDenseStorage()
        )
    );
    for (int i = 0; i < valsToCheck[valsToCheck.length - 1]; ++i) {
      collector.add(fn.hashLong(random.nextLong()).asBytes());
      if (i == valsToCheck[valsToCheckIndex]) {
        Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
        ++valsToCheckIndex;
      }
    }
    Assert.assertEquals(expectedVals.length, valsToCheckIndex + 1);
    Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
  }

  @Test
  public void testEstimationLimitDifferentFromCapacity() throws Exception
  {
    Random random = new Random(0L);

    final int[] valsToCheck = {10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000};
    final double[] expectedVals = {
        11.029647221949576, 21.108407720752034, 51.64575281885815, 100.42231726408892,
        981.8579991802412, 1943.1337257462792, 4946.192042635218, 9935.088157579434,
        20366.1486889433, 49433.56029693898, 100615.26273314281, 980831.624899156000,
        1982408.2608981386
    };

    int valsToCheckIndex = 0;
    HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(
        (ByteBuffer) ByteBuffer.allocate(10000)
                               .position(0)
                               .limit(HyperLogLogCollector.getLatestNumBytesForDenseStorage())
    );
    for (int i = 0; i < valsToCheck[valsToCheck.length - 1]; ++i) {
      collector.add(fn.hashLong(random.nextLong()).asBytes());
      if (i == valsToCheck[valsToCheckIndex]) {
        Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
        ++valsToCheckIndex;
      }
    }
    Assert.assertEquals(expectedVals.length, valsToCheckIndex + 1);
    Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
  }

  @Test
  public void testSparseEstimation() throws Exception
  {
    final Random random = new Random(0);
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      collector.add(fn.hashLong(random.nextLong()).asBytes());
    }

    Assert.assertEquals(
        collector.estimateCardinality(), HyperLogLogCollector.estimateByteBuffer(collector.toByteBuffer()), 0.0d
    );
  }

  @Test
  public void testHighBits() throws Exception
  {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    // fill up all the buckets so we reach a registerOffset of 49
    fillBuckets(collector, (byte) 0, (byte) 49);

    // highest possible bit position is 64
    collector.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    Assert.assertEquals(8.5089685793441677E17, collector.estimateCardinality(), 1000);

    // this might happen once in a million years if you hash a billion values a second
    fillBuckets(collector, (byte) 0, (byte) 63);
    collector.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

    Assert.assertEquals(Double.POSITIVE_INFINITY, collector.estimateCardinality(), 1000);
  }

  @Test
  public void testMaxOverflow()
  {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.add((short)23, (byte)16);
    Assert.assertEquals(23, collector.getMaxOverflowRegister());
    Assert.assertEquals(16, collector.getMaxOverflowValue());
    Assert.assertEquals(0, collector.getRegisterOffset());
    Assert.assertEquals(0, collector.getNumNonZeroRegisters());

    collector.add((short)56, (byte)17);
    Assert.assertEquals(56, collector.getMaxOverflowRegister());
    Assert.assertEquals(17, collector.getMaxOverflowValue());

    collector.add((short)43, (byte)16);
    Assert.assertEquals(56, collector.getMaxOverflowRegister());
    Assert.assertEquals(17, collector.getMaxOverflowValue());
    Assert.assertEquals(0, collector.getRegisterOffset());
    Assert.assertEquals(0, collector.getNumNonZeroRegisters());
  }

  @Test
  public void testMergeMaxOverflow()
  {
    // no offset
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.add((short)23, (byte)16);

    HyperLogLogCollector other = HyperLogLogCollector.makeLatestCollector();
    collector.add((short)56, (byte)17);

    collector.fold(other);
    Assert.assertEquals(56, collector.getMaxOverflowRegister());
    Assert.assertEquals(17, collector.getMaxOverflowValue());

    // different offsets
    // fill up all the buckets so we reach a registerOffset of 49
    collector = HyperLogLogCollector.makeLatestCollector();
    fillBuckets(collector, (byte) 0, (byte) 49);
    collector.add((short)23, (byte)65);

    other = HyperLogLogCollector.makeLatestCollector();
    fillBuckets(other, (byte) 0, (byte) 43);
    other.add((short)47, (byte)67);

    collector.fold(other);
    Assert.assertEquals(47, collector.getMaxOverflowRegister());
    Assert.assertEquals(67, collector.getMaxOverflowValue());
  }


  private static void fillBuckets(HyperLogLogCollector collector, byte startOffset, byte endOffset)
  {
    byte offset = startOffset;
    while (offset <= endOffset) {
      // fill buckets to shift registerOffset
      for (short bucket = 0; bucket < 2048; ++bucket) {
        collector.add(bucket, offset);
      }
      offset++;
    }
  }

  @Test
  public void testFoldOrder() throws Exception
  {
    final List<String> objects = Lists.newArrayList(
        "AQcH/xYEMXOjRTVSQ1NXVENEM1RTUlVTRDI1aEVnhkOjNUaCI2MkU2VVhVNkNyVTa4NEYkS0kjZYU1RDdEYzUjglNTUzVFM0NkU3ZFUjOVJCdlU0N2QjRDRUV1MyZjNmVDOUM2RVVFRzhnUzVXY1R1RHUnNziURUdmREM0VjVEQmU0aEInZYNzNZNVRFgzVFNolSJHNIQ3QklEZlNSNoNTJXpDk1dFWjJGNYNiQzQkZFNEYzc1NVhSczM2NmJDZlc3JJRCpVNiRlNEI3dmU1ZGI0Q1RCMhNFZEJDZDYyNFOCM3U0VmRlVlNIRVQ4VVw1djNDVURHVSaFU0VEY0U1JFNIVCYlVEJWM2NWU0eURDOjQ6YyNTYkZjNUVjR1ZDdnVkMzVHZFpjMzlmNEFHM0dHJlRYTHSEQjVZVVZkVVIzIjg2SUU0NSM0VFNDNCdGVlQkhBNENCVTZGZEVlxFQyQ0NYWkUmVUJUYzRlNqg4NVVTNThEJkRGNDNUNFSEYmgkR0dDR1JldCNhVEZGRENGc1NDRUNER3WJRTRHQ4JlOYZoJDVVVVMzZSREZ1Q1UjSHNkdUMlU0ODIzZThSNmNDNjQ1o2I0YiRGYyZkNUJYVEMyN2QpQyMkc2VTE4U2VCNHZFRDNTh0IzI2VFNTMlUkNGMlKTRCIyR3QiQzFUNkRTdDM6RDRFI3VyVlcyWCUlQ0YjNjU2Q2dEVFNTRyRlI7VElHVTVVNGk0JHJTQzQkQyVlV0NCVlRkhWYkQ0RVaDNYdFZHWEWFJEYpM0QjNjNVUzNCVzVkgzZGFzQkRZUzN2U1dUFGVWZTUzVUREZDciZEVVYVNjeCU0ZDdEhzIpU2RTOFRUQkWlk1OFRUVTN1MkZSM3ZFc1VDNnUmc2NKNUaUIzd3M0RWxEZTsiNENLVHU0NFUmQ2RWRFdCNUVENFkxZCEnRLQkNEU0RVNmVDQjl9ZmNkM1QVM0MzQkUjJlVHRkNEVWlENDVUIlUvRkM0RVY1UzY6OGVHVCRDIzRUUlUjM2RDWSVkVIU1U1ZiVFNlNDhTN1VWNTVEZ2RzNzVDQlY0ZUNENUM5NUdkRDJGYzRCUzIjRGR4UmJFI4GDRTUiQ0ZUhVY1ZEYoZSRoVDYnREYkQ1SUU0RWUycjp2RZIySVZkUmZDREZVJGQyVEc1JElBZENEU2VEQlVUUnNDQziLRTNidmNjVCtjRFU2Q0SGYzVHVpGTNoVDxFVSMlWTJFQyRJdV1EI3RDloYyNFQ0c1NVY0ZHVEY0dkM2QkQyVDVUVTNFUyamMUdSNrNz0mlFlERzZTSGhFRjVGM3NWU2NINDI2U1RERUhjY4FHNWNTVTV1U0U2I0VXNEZERWNDNUSjI1WmMmQ4U=",
        "AQgH+BUFUEUrZVRjM2IjMzJRESMlUnlTJjEjRhRlNBEyMSUpaGJTMjRCIzMTNCRENRdxNiNEZCQzNERYMiAyIiQmUTI+MhEzV1RWJoMjQjIySDN0QiYDUjUzNjRUVEYyQleDEiUmg0ERRjIjIzJUQjMxNlJGUTNDJFNTRzJiE1M0RjQzUzIiFDUmMjIzJWVCNENTIRJVODUzEkIVMhFEIjM0MkMyIRRCNFNxQyNCQ2UzOFQiJSM0EzU1V1M2EjhUVENDclZzImEiMTJBQlQiJCgyIyKkJSUlNBNDE2M3QSIyMicjMlJEUhJDJFQjJ0VSQ0QyYSFhZSNlQ4REUzVFIlOFRHIkYUJEM8RVMkMiMEczQwMlE1EkAlNiQlhCNkISRVI0ITUjRDU1JVNlK1QyGGRHQVM0NUVHQ1MkMyQoIzMzFCFUI0IhU1OIhCIlZUQVIUMyYzMlMUZ0RCKEIigUIlQ0QkQTM0MkM0QyJkUSM2I2tHJDUTQ0RBQ0YyNlUxUzIiIiMUiSMzUlJDNDQjM0ITQyNIM1MyNWM0MDOTZYVDRWIiZhMzc0NCJ0Q0NDZEMUElMyRyMmUhNiMkIZNjMkEyRTIzYkMzNUODUTNDJVM0ZTQjFCJCNWSTUlEiNCM1U2FCZUJzMVMyLjNkMhITVDEjIYMzNiVmIlO1VTMjMiVDQ2NTJFYyE0Q2IjRDN2IjRTRUVTFUVEYVKBVSMVJSFE0zOXNSJIqVElMVM4MiZEFSMhRlJEJUZnMycmQmQyJDl1JzVjMXQ0MzMjE1VUI1JDJUQyYRQ2JVZzQUJDM2IyInEkY1QiZTJEMRMiMxRVNEUjJUNkJHNSQiNCVCIyIjJUQlEhNUdFUhQzgkcSZaJUVUM0YiJEM2SjczUUIUIlQiM0RiQkIzZhRBJSRzQ0ZUI00UUSRSQlQmMkNINzODQhJFRTZ0FRQ3QTRhIzFTJFRBMmMzQzQhZENUMiIlV2VEMiNFRWQ1F1IyFXRSUyRTMqZ3I0YyhUNEJRMjISZRc2NDOEIjIxVGVWIXYyMiNCJBFDQSMhIzMjVFIDElgyJCUyVFgkRSQzIjJFQlNWRTQWMmQzFFOiMzVTZGMxNFZUNmIjRjETNUNURERTQjYVIkEzNEEyNDNTVUJSVzVkMjEyUlMjQ0RGgyFFNUQhRGMmRUQ2ZSOFETUYNlZCUhRiU2QhVUUiIlJDRjMhRVJDZxNSRTNBRCEoI0FGNUVRE0VFOGdCRDM2QkJCFSQhMxITRoE0VFIzVWUiUTNkRhNDMiMmIzRDQSNTFDoldaJDcnNjkSMJg3IkIiRENSQmciUhY2NFQ4RSNoJENkWDMmVCJGMxQjJGJScyNTJDVDNEEiZSMzQyIyVGRTNEIUw=",
        "AQgH+hQAFyMzlFVXNCNlRxRUYlRUUUZCMnRFJiR0WTgyZiRJZzRFQkVTVVVWc2ZFMlY1QkIxYUQTI0JDY1YkNEVENGUuQTRiNkQ0VUEzNkKUKLSIVkUhNiZURnRFMzcjVEBTdjVVVCIzJDM0hjc0RDVlVjRqMjJVZTNSM0QmQyMTRlNzVCNERFQyMxNBZHMiUSdYIUUjNlVjNzRyYWFHRHI3hKMnYnhFNCZOdlNUZBM0Q0clNTVBiEQRMUQzNSNVQ0IkEmZYNzIyNkRSUik2VBOVRCRDg0IilEMlcjRJMkJDSjRCJURTVDJBMmRTVBM1YyRRMSQoRDV2YzRDVCUkQWFFNDYnQ0IkUzRjRkQ1dGI0VUYzRERCQ1I2dFNhREOUUjJDc0NTN0JFNUZJRGFpU1Q0QyJlNiMzNCZSKFQzYnNUWTMiRGMiRWdSQzMiQnQ0QSgjVUMiE0hRM1NVUiZVIlRkRVMzI2VkRjQWQ1YyRiZWNHQXQ0UllUMSVTJDQzMkWCQiRFglMzIzKEYzJSJFMyREVIQlVFFlYzMDQyVWUZNCQlM0NUJFIkWiNnREdEJDImNWJDOIcmKyQzc5VDVRQ3PVNjQzIkJTQ3FzMjMyRFVFVTUlNUZEMzEjI0Q0M0Y2U1JTREQjIhZScUJjQkYhFRJFQyI0pTVmFTVlMkJXNDI1U3dFZkR2U0NCVRQyRih0UkIhckRUY0ZHSG00EiJUdVIxVjVGNnUVZCxEQkNTQjQ0IkZDciIkODYxM1MzRZRHQxVEZHZWJFIzRRZjVDNBMzI1Q1FEhUMiI0NkJWJWJDJzYlQiRSQjRoRiRhJTIjNSRVJEM1MiYmUiNBkjFkczRWU1SURIJUVDRFQ0QyZCUlRENEImE2FDQxRjlEdTI3RSNEU3RGJyWDNVMTVJNDM1QkJFQmNWRXUlcxNEQzNTGCtDUlNDMzMzY2VlcUQlaUIyZVMzA3NFM1NDc0JjZDUkQiFDY3QUczQzUkVDQjMiUWQ0NEQyNRVTMRJFM2RUMZNSQkQ0MkIiUgGCUkRig1UiElQkdDJFJDciVGIxMjQzI1UlNlRTM1JkRDc+RSM0VFUzMjWCU0RDMxJyJVJGI1VTEUQyM1R0I1c0NFNTM3MhIlUkNFIlZGNURkVURyNIVCMyYzQmQjITRkVHQ2NINGQ0Y0UW0icyUzMydEVBJVJIJENkUjRVIjQSNVYnEzVYMzUmYzGVNFRiQk0iVTVCM0RjJSMyRWRSQkURJBR0M0NzhnRlM3IzQxMTRDJjM1UVUkJCNUQTVGQlEzN0VDMyM0MmO2QoQzNSVURhFEAkU2IldINHRUU00zNFJVQxUkZEcVMyJSJkQjKFNCNUOzIYJEHEUyKCQjJESSY=",
        "AQcH/x0BbjQ2JTpUlkdFRERHVDRkWDU0RGR1ejRURHZ6IzUqdJN1M1VFQiNHI1NTI0J1VHOGZYVFVTRIRJVkVmUolWVShERjSDRVMlRlJDU2VFh3UmR1Mjg3K0M2SUY0Q0ZUspNiJEdZMmc3YkxGSERGOGdjgzNRVGM1Q1UnN0RHU1Y0WWUzRWVEJSRSeGQ0RlNFJVVJU3YoQkdEQ2M2MiVFUyJWRUVWNmRkM0NkVER2WXNkR0QlNGNEVlYzZSS4RDMyVEQ1ckRTM0ZoMlQ2tURGQ0OFQ0ZiY1ZFNEajdXEjVSI6ZWSjNHVRRTRVMldzUjm0NGU0dlhESFRDM0IzVCYkdjdlJJRFVDaHEzUkRmNWOEVXZTM0U0VkREdSUjRHVVViVCVFVUN0RDNDkl01VHMoNVQzYlZFZmNVVUNDQ1VjUiQ2NTV0UzZVModSNEY4Zpc2JjhjiFJVUGM0SHI0UzRTU1R2R0d3VENUZSQzRUZlY4d0aGNkhTQzWVZFZTZkJ2NEZaVDU1alJWpFJpRGRnIlZUU1ZUR2M1NzOkVEMzVjZERiVlRYSEkmU4RLM0RTQ2Q2RjM3RTNhdVVEQzRXJUZTRmM1OEZTYyJkRGRjZDVTlDhSMzdXQiU1RFUiIoRpVGlXIjY1UVVjc0RDJDNSM0NVJTNkRUU1U0lDdEVXY2NGVVNJVmJJRTREVVNiMyVIQ3U6O0U0M0MzZFVVIzJmNERWJaJjikIlRXk1hFQ2NEU0RUN4UzdENEsVgzZFVidXUnU2VRZFRUQmZmRERCQ0ZER2Q3YnZFNlVpJUkzZVREKFWEUzVVMzYzQzQhfTYzQ0IlI5UoV0RGJCVXSDkyZCRSU3ITUkNoYzJUMkYzhlVVRTNyaDNmQzRDVVRjNkVUhEJyRBR2JlOEREVUU0RjY4Nkc3ZERGUyVDNFZGNFOTY3U1OKNlkjQy1TVlRTQ0M1REU2QhgzUzUzOWlWQ1Z3RTQzIzc7RXVkI0M4NCNYRVRGNEZbhFEyVJI0R1OUZEQ3VUVEQlU1NkNYJEYzdSQ0ZSNGeEWIVVU3KEVFY1RZQ0JSNEJFNFMyM0UzN0hHNTQjMlRGNkiEMyVjRFNVRXNkZGM2M4hENCMnU1VWQjNFRkO2VmO1RndEVzWTQiiHQ0NzM2clM4NjQxpjQjZEVTNEpEdlREJzc3OjZnRlNFNWJVNFeDokNCRmQ5NURJVUZSJyRDRXikVURVITZDNGW0ITNEOUQ0RUklZDQjYjVENURDRCRmRDU1hCY2VTR0RGIzJSZzlSczdTFJJkRlZyU1M1JTdVhDYhVFczQ0hTRIc0RCNDdUJEQxNlZEQ2ZEUiJJRFU3YzVGRER0R2ZlNFOTU1MyRGI0RzMkQ2Q="
    );

    List<HyperLogLogCollector> collectors = Lists.transform(
        objects, new Function<String, HyperLogLogCollector>()
        {
          @Nullable
          @Override
          public HyperLogLogCollector apply(
              @Nullable String s
          )
          {
            return HyperLogLogCollector.makeCollector(
                ByteBuffer.wrap(Base64.decodeBase64(s))
            );
          }
        }
    );

    Collection<List<HyperLogLogCollector>> permutations = Collections2.permutations(collectors);

    for(List<HyperLogLogCollector> permutation : permutations) {
      HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

      for (HyperLogLogCollector foldee : permutation) {
        collector.fold(foldee);
      }
      Assert.assertEquals(29, collector.getMaxOverflowValue());
      Assert.assertEquals(366, collector.getMaxOverflowRegister());
      Assert.assertEquals(1.0429189446653817E7, collector.estimateCardinality(), 1);
    }
  }

  // Provides a nice printout of error rates as a function of cardinality
  @Ignore
  @Test
  public void showErrorRate() throws Exception
  {
    HashFunction fn = Hashing.murmur3_128();
    Random random = new Random();

    double error = 0.0d;
    int count = 0;

    final int[] valsToCheck = {
        10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000, 10000000, Integer.MAX_VALUE
    };

    for (int numThings : valsToCheck) {
      long startTime = System.currentTimeMillis();
      HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        if (i != 0 && i % 100000000 == 0) {
          ++count;
          error = computeError(error, count, i, startTime, collector);
        }
        collector.add(fn.hashLong(random.nextLong()).asBytes());
      }

      ++count;
      error = computeError(error, count, numThings, startTime, collector);
    }
  }

  private double computeError(double error, int count, int numThings, long startTime, HyperLogLogCollector collector)
  {
    final double estimatedValue = collector.estimateCardinality();
    final double errorThisTime = Math.abs((double) numThings - estimatedValue) / numThings;

    error += errorThisTime;

    System.out.printf(
        Locale.ENGLISH,
        "%,d ==? %,f in %,d millis. actual error[%,f%%], avg. error [%,f%%]%n",
        numThings,
        estimatedValue,
        System.currentTimeMillis() - startTime,
        100 * errorThisTime,
        (error / count) * 100
    );
    return error;
  }
}
