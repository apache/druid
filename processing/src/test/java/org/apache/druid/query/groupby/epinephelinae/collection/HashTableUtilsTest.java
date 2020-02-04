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

package org.apache.druid.query.groupby.epinephelinae.collection;

import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class HashTableUtilsTest
{
  @Test
  public void test_previousPowerOfTwo()
  {
    final Int2IntMap expectedResults = new Int2IntLinkedOpenHashMap();
    expectedResults.put(Integer.MIN_VALUE, Integer.MIN_VALUE);
    expectedResults.put(Integer.MIN_VALUE + 1, Integer.MIN_VALUE);
    expectedResults.put(-4, Integer.MIN_VALUE);
    expectedResults.put(-3, Integer.MIN_VALUE);
    expectedResults.put(-2, Integer.MIN_VALUE);
    expectedResults.put(-1, Integer.MIN_VALUE);
    expectedResults.put(0, Integer.MIN_VALUE);
    expectedResults.put(1, 1);
    expectedResults.put(2, 2);
    expectedResults.put(3, 2);
    expectedResults.put(4, 4);
    expectedResults.put(5, 4);
    expectedResults.put(6, 4);
    expectedResults.put(7, 4);
    expectedResults.put(8, 8);
    expectedResults.put((1 << 30) - 1, 1 << 29);
    expectedResults.put(1 << 30, 1 << 30);
    expectedResults.put((1 << 30) + 1, 1073741824);
    expectedResults.put(Integer.MAX_VALUE - 1, 1073741824);
    expectedResults.put(Integer.MAX_VALUE, 1073741824);

    for (final Int2IntMap.Entry entry : expectedResults.int2IntEntrySet()) {
      Assert.assertEquals(
          entry.getIntKey() + " => " + entry.getIntValue(),
          entry.getIntValue(),
          HashTableUtils.previousPowerOfTwo(entry.getIntKey())
      );
    }
  }

  private static WritableMemory generateRandomButNotReallyRandomMemory(final int length)
  {
    final WritableMemory randomMemory = WritableMemory.allocate(length);

    // Fill with random bytes, but use the same seed every run for consistency. This test should pass with
    // any seed unless something really pathological happens.
    final Random random = new Random(0);
    final byte[] randomBytes = new byte[length];
    random.nextBytes(randomBytes);
    randomMemory.putByteArray(0, randomBytes, 0, length);
    return randomMemory;
  }

  @Test
  public void test_hashMemory_allByteLengthsUpTo128()
  {
    // This test validates that we *can* hash any amount of memory up to 128 bytes, and that if any bit is flipped
    // in the memory then the hash changes. It doesn't validate that the hash function is actually good at dispersion.
    // That also has a big impact on performance and needs to be checked separately if the hash function is changed.

    final int maxBytes = 128;
    final WritableMemory randomMemory = generateRandomButNotReallyRandomMemory(maxBytes);

    for (int numBytes = 0; numBytes < maxBytes; numBytes++) {
      // Grab "numBytes" bytes from the end of randomMemory.
      final Memory regionToHash = randomMemory.region(maxBytes - numBytes, numBytes);

      // Validate that hashing regionAtEnd is equivalent to hashing the end of a region. This helps validate
      // that using a nonzero position is effective.
      Assert.assertEquals(
          StringUtils.format("numBytes[%s] nonzero position check", numBytes),
          HashTableUtils.hashMemory(regionToHash, 0, numBytes),
          HashTableUtils.hashMemory(randomMemory, maxBytes - numBytes, numBytes)
      );

      // Copy the memory and make sure we did it right.
      final WritableMemory copyOfRegion = WritableMemory.allocate(numBytes);
      regionToHash.copyTo(0, copyOfRegion, 0, numBytes);
      Assert.assertTrue(
          StringUtils.format("numBytes[%s] copy equality check", numBytes),
          regionToHash.equalTo(0, copyOfRegion, 0, numBytes)
      );

      // Validate that flipping any bit affects the hash.
      for (int bit = 0; bit < numBytes * Byte.SIZE; bit++) {
        final int bytePosition = bit / Byte.SIZE;
        final byte mask = (byte) (1 << (bit % Byte.SIZE));

        copyOfRegion.putByte(
            bytePosition,
            (byte) (copyOfRegion.getByte(bytePosition) ^ mask)
        );

        Assert.assertNotEquals(
            StringUtils.format("numBytes[%s] bit[%s] flip check", numBytes, bit),
            HashTableUtils.hashMemory(regionToHash, 0, numBytes),
            HashTableUtils.hashMemory(copyOfRegion, 0, numBytes)
        );

        // Set it back and make sure we did it right.
        copyOfRegion.putByte(
            bytePosition,
            (byte) (copyOfRegion.getByte(bytePosition) ^ mask)
        );

        Assert.assertTrue(
            StringUtils.format("numBytes[%s] bit[%s] reset check", numBytes, bit),
            regionToHash.equalTo(0, copyOfRegion, 0, numBytes)
        );
      }
    }
  }

  @Test
  public void test_memoryEquals_allByteLengthsUpTo128()
  {
    // This test validates that we can compare any two slices of memory of size up to 128 bytes, and that if any bit
    // is flipped in two identical memory slices, then the comparison correctly returns not equal.

    final int maxBytes = 128;
    final WritableMemory randomMemory = generateRandomButNotReallyRandomMemory(maxBytes);

    for (int numBytes = 0; numBytes < maxBytes; numBytes++) {
      // Copy "numBytes" from the end of randomMemory.
      final WritableMemory copyOfRegion = WritableMemory.allocate(numBytes);
      randomMemory.copyTo(maxBytes - numBytes, copyOfRegion, 0, numBytes);

      // Compare the two.
      Assert.assertTrue(
          StringUtils.format("numBytes[%s] nonzero position check", numBytes),
          HashTableUtils.memoryEquals(randomMemory, maxBytes - numBytes, copyOfRegion, 0, numBytes)
      );

      // Validate that flipping any bit affects equality.
      for (int bit = 0; bit < numBytes * Byte.SIZE; bit++) {
        final int bytePosition = bit / Byte.SIZE;
        final byte mask = (byte) (1 << (bit % Byte.SIZE));

        copyOfRegion.putByte(
            bytePosition,
            (byte) (copyOfRegion.getByte(bytePosition) ^ mask)
        );

        Assert.assertFalse(
            StringUtils.format("numBytes[%s] bit[%s] flip check", numBytes, bit),
            HashTableUtils.memoryEquals(randomMemory, maxBytes - numBytes, copyOfRegion, 0, numBytes)
        );

        // Set it back and make sure we did it right.
        copyOfRegion.putByte(
            bytePosition,
            (byte) (copyOfRegion.getByte(bytePosition) ^ mask)
        );

        Assert.assertTrue(
            StringUtils.format("numBytes[%s] bit[%s] reset check", numBytes, bit),
            HashTableUtils.memoryEquals(randomMemory, maxBytes - numBytes, copyOfRegion, 0, numBytes)
        );
      }
    }
  }
}
