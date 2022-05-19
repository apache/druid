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

import org.apache.datasketches.memory.Memory;

public class HashTableUtils
{
  private HashTableUtils()
  {
    // No instantiation.
  }

  /**
   * Computes the previous power of two less than or equal to a given "n".
   *
   * The integer should be between 1 (inclusive) and {@link Integer#MAX_VALUE} for best results. Other parameters will
   * return {@link Integer#MIN_VALUE}.
   */
  public static int previousPowerOfTwo(final int n)
  {
    if (n > 0) {
      return Integer.highestOneBit(n);
    } else {
      return Integer.MIN_VALUE;
    }
  }

  /**
   * Compute a simple, fast hash code of some memory range.
   *
   * @param memory   a region of memory
   * @param position position within the memory region
   * @param length   length of memory to hash, starting at the position
   */
  public static int hashMemory(final Memory memory, final long position, final int length)
  {
    // Special cases for small, common key sizes to speed them up: e.g. one int key, two int keys, one long key, etc.
    // The plus-one sizes (9, 13) are for nullable dimensions. The specific choices of special cases were chosen based
    // on benchmarking (see MemoryBenchmark) on a Skylake-based cloud instance.

    switch (length) {
      case 4:
        return memory.getInt(position);

      case 8:
        return 31 * (31 + memory.getInt(position)) + memory.getInt(position + Integer.BYTES);

      case 9:
        return 31 * (31 * (31 + memory.getInt(position)) + memory.getInt(position + Integer.BYTES))
               + memory.getByte(position + 2L * Integer.BYTES);

      case 12:
        return 31 * (31 * (31 + memory.getInt(position)) + memory.getInt(position + Integer.BYTES))
               + memory.getInt(position + 2L * Integer.BYTES);

      case 13:
        return 31 * (31 * (31 * (31 + memory.getInt(position)) + memory.getInt(position + Integer.BYTES))
                     + memory.getInt(position + 2L * Integer.BYTES)) + memory.getByte(position + 3L * Integer.BYTES);

      case 16:
        return 31 * (31 * (31 * (31 + memory.getInt(position)) + memory.getInt(position + Integer.BYTES))
                     + memory.getInt(position + 2L * Integer.BYTES)) + memory.getInt(position + 3L * Integer.BYTES);

      default:
        int hashCode = 1;
        int remainingBytes = length;
        long pos = position;

        while (remainingBytes >= Integer.BYTES) {
          hashCode = 31 * hashCode + memory.getInt(pos);
          remainingBytes -= Integer.BYTES;
          pos += Integer.BYTES;
        }

        if (remainingBytes == 1) {
          hashCode = 31 * hashCode + memory.getByte(pos);
        } else if (remainingBytes == 2) {
          hashCode = 31 * hashCode + memory.getByte(pos);
          hashCode = 31 * hashCode + memory.getByte(pos + 1);
        } else if (remainingBytes == 3) {
          hashCode = 31 * hashCode + memory.getByte(pos);
          hashCode = 31 * hashCode + memory.getByte(pos + 1);
          hashCode = 31 * hashCode + memory.getByte(pos + 2);
        }

        return hashCode;
    }
  }

  /**
   * Compare two memory ranges for equality.
   *
   * The purpose of this function is to be faster than {@link Memory#equalTo} for the small memory ranges that
   * typically comprise keys in hash tables. As of this writing, it is. See "MemoryBenchmark" in the druid-benchmarks
   * module for performance evaluation code.
   *
   * @param memory1 a region of memory
   * @param offset1 position within the first memory region
   * @param memory2 another region of memory
   * @param offset2 position within the second memory region
   * @param length  length of memory to compare, starting at the positions
   */
  public static boolean memoryEquals(
      final Memory memory1,
      final long offset1,
      final Memory memory2,
      final long offset2,
      final int length
  )
  {
    // Special cases for small, common key sizes to speed them up: e.g. one int key, two int keys, one long key, etc.
    // The plus-one sizes (9, 13) are for nullable dimensions. The specific choices of special cases were chosen based
    // on benchmarking (see MemoryBenchmark) on a Skylake-based cloud instance.

    switch (length) {
      case 4:
        return memory1.getInt(offset1) == memory2.getInt(offset2);

      case 8:
        return memory1.getLong(offset1) == memory2.getLong(offset2);

      case 9:
        return memory1.getLong(offset1) == memory2.getLong(offset2)
               && memory1.getByte(offset1 + Long.BYTES) == memory2.getByte(offset2 + Long.BYTES);

      case 12:
        return memory1.getInt(offset1) == memory2.getInt(offset2)
               && memory1.getLong(offset1 + Integer.BYTES) == memory2.getLong(offset2 + Integer.BYTES);

      case 13:
        return memory1.getLong(offset1) == memory2.getLong(offset2)
               && memory1.getInt(offset1 + Long.BYTES) == memory2.getInt(offset2 + Long.BYTES)
               && (memory1.getByte(offset1 + Integer.BYTES + Long.BYTES)
                   == memory2.getByte(offset2 + Integer.BYTES + Long.BYTES));

      case 16:
        return memory1.getLong(offset1) == memory2.getLong(offset2)
               && memory1.getLong(offset1 + Long.BYTES) == memory2.getLong(offset2 + Long.BYTES);

      default:
        return memory1.equalTo(offset1, memory2, offset2, length);
    }
  }
}
