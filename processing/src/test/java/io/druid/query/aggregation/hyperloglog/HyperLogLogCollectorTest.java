/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.hyperloglog;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
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
  @Ignore @Test
  public void testHighCardinalityRollingFold() throws Exception
  {
    final HyperLogLogCollector rolling = HyperLogLogCollector.makeLatestCollector();
    final HyperLogLogCollector simple = HyperLogLogCollector.makeLatestCollector();

    MessageDigest md = MessageDigest.getInstance("SHA-1");
    HyperLogLogCollector tmp = HyperLogLogCollector.makeLatestCollector();

    int count;
    for (count = 0; count < 100_000_000; ++count) {
      md.update(Integer.toString(count).getBytes());

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
        String.format(
            "Rolling cardinality estimate off by %4.1f%%",
            100 * (1 - rolling.estimateCardinality() / n)
        )
    );

    Assert.assertEquals(n, simple.estimateCardinality(), n * 0.05);
    Assert.assertEquals(n, rolling.estimateCardinality(), n * 0.05);
  }

  @Ignore @Test
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
    System.out.printf("testHighCardinalityRollingFold2 took %d ms%n", System.currentTimeMillis() - start);

    int n = count;

    System.out.println("True cardinality " + n);
    System.out.println("Rolling buffer cardinality " + rolling.estimateCardinality());
    System.out.println(
        String.format(
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
    numNonZero += (HyperLogLogCollector.NUM_BYTES_FOR_BUCKETS - initialBytes.length) * numNonZeroInRemaining;

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
        md.update(Integer.toString(random.nextInt()).getBytes());
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
    Random random = new Random(0l);

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
    Random random = new Random(0l);

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
    Random random = new Random(0l);

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
        collector.estimateCardinality(), collector.estimateByteBuffer(collector.toByteBuffer()), 0.0d
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

    Assert.assertEquals(Double.MAX_VALUE, collector.estimateCardinality(), 1000);
  }

  @Test
  public void testCompare1() throws Exception
  {
    HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
    HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
    collector1.add(fn.hashLong(0).asBytes());
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 100; i = i + 2) {
      collector1.add(fn.hashLong(i).asBytes());
      collector2.add(fn.hashLong(i + 1).asBytes());
      Assert.assertEquals(1, comparator.compare(collector1, collector2));
      Assert.assertEquals(1, Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()));
    }
  }

  @Test
  public void testCompare2() throws Exception
  {
    Random rand = new Random(0);
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 1000; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(50);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 1 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 100; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(500);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 2 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 10; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(100000);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 20000 + rand.nextInt(100000);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }
  }

  @Test
  public void testMaxOverflow() {
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
  public void testMergeMaxOverflow() {
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

  // Provides a nice printout of error rates as a function of cardinality
  @Ignore @Test
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
