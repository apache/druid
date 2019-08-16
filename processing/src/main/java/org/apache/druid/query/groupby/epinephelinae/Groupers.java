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

package org.apache.druid.query.groupby.epinephelinae;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class Groupers
{
  private Groupers()
  {
    // No instantiation
  }

  private static final AggregateResult DICTIONARY_FULL_ZERO_COUNT = AggregateResult.partial(
      0,
      "Not enough dictionary space to execute this query. Try increasing "
      + "druid.query.groupBy.maxMergingDictionarySize or enable disk spilling by setting "
      + "druid.query.groupBy.maxOnDiskStorage to a positive number."
  );

  private static final AggregateResult HASH_TABLE_FULL_ZERO_COUNT = AggregateResult.partial(
      0,
      "Not enough aggregation buffer space to execute this query. Try increasing "
      + "druid.processing.buffer.sizeBytes or enable disk spilling by setting "
      + "druid.query.groupBy.maxOnDiskStorage to a positive number."
  );

  private static final int USED_FLAG_MASK = 0x7fffffff;

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  /**
   * This method was rewritten in Java from an intermediate step of the Murmur hash function in
   * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp, which contained the
   * following header:
   *
   * MurmurHash3 was written by Austin Appleby, and is placed in the public domain. The author
   * hereby disclaims copyright to this source code.
   */
  private static int smear(int hashCode)
  {
    return C2 * Integer.rotateLeft(hashCode * C1, 15);
  }

  public static AggregateResult dictionaryFull(final int count)
  {
    if (count == 0) {
      return DICTIONARY_FULL_ZERO_COUNT;
    } else {
      return AggregateResult.partial(count, DICTIONARY_FULL_ZERO_COUNT.getReason());
    }
  }

  public static AggregateResult hashTableFull(final int count)
  {
    if (count == 0) {
      return HASH_TABLE_FULL_ZERO_COUNT;
    } else {
      return AggregateResult.partial(count, HASH_TABLE_FULL_ZERO_COUNT.getReason());
    }
  }

  public static int hashObject(final Object obj)
  {
    // Mask off the high bit so we can use that to determine if a bucket is used or not.
    // Also apply the "smear" function, to improve distribution.
    return smear(obj.hashCode()) & USED_FLAG_MASK;
  }

  public static int hashIntArray(final int[] ints, final int start, final int length)
  {
    // Similar to what Arrays.hashCode would do.
    // Also apply the "smear" function, to improve distribution.
    int hashCode = 1;
    for (int i = 0; i < length; i++) {
      hashCode = 31 * hashCode + ints[start + i];
    }

    return smear(hashCode) & USED_FLAG_MASK;
  }

  static int getUsedFlag(int keyHash)
  {
    return keyHash | 0x80000000;
  }

  public static ByteBuffer getSlice(ByteBuffer buffer, int sliceSize, int i)
  {
    final ByteBuffer slice = buffer.duplicate();
    slice.position(sliceSize * i);
    slice.limit(slice.position() + sliceSize);
    return slice.slice();
  }

  /**
   * Write ints from "start" to "end" into "scratch", if start != 0. Otherwise, return null.
   */
  @Nullable
  public static int[] writeAggregationRows(final int[] scratch, final int start, final int end)
  {
    if (start == 0) {
      return null;
    } else {
      final int numRows = end - start;
      for (int i = 0; i < numRows; i++) {
        scratch[i] = start + i;
      }

      return scratch;
    }
  }
}
