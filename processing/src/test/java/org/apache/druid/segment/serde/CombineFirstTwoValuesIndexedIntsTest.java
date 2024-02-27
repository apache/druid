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

package org.apache.druid.segment.serde;

import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Function;

/**
 * Test for {@link CombineFirstTwoValuesIndexedInts}.
 */
public class CombineFirstTwoValuesIndexedIntsTest
{
  @Test
  public void testCombineFirstTwoValues()
  {
    // (expectedCombined, original)
    assertCombine(new int[]{0, 1, 2}, new int[]{1, 2, 3});
    assertCombine(new int[]{0, 0, 1, 2}, new int[]{0, 1, 2, 3});
    assertCombine(new int[]{2, 0, 1, 0, 4, 0}, new int[]{3, 0, 2, 1, 5, 0});
  }

  private static void assertCombine(final int[] expectedCombined, final int[] original)
  {
    assertCombine(
        expectedCombined,
        original,
        arr -> new CombineFirstTwoValuesIndexedInts(new ArrayBasedIndexedInts(arr))
    );
  }

  static void assertCombine(
      final int[] expectedCombined,
      final int[] original,
      final Function<int[], IndexedInts> combineFn
  )
  {
    final IndexedInts combined = combineFn.apply(original);

    // Check size.
    Assert.assertEquals(
        StringUtils.format("%s (size)", Arrays.toString(original)),
        expectedCombined.length,
        combined.size()
    );

    // Check regular get.
    final int[] arr = new int[expectedCombined.length];
    for (int i = 0; i < expectedCombined.length; i++) {
      arr[i] = combined.get(i);
    }
    Assert.assertArrayEquals(StringUtils.format("%s (get)", Arrays.toString(original)), expectedCombined, arr);

    // Check contiguous vector get.
    Arrays.fill(arr, Integer.MIN_VALUE);
    combined.get(arr, 0, arr.length);
    Assert.assertArrayEquals(
        StringUtils.format("%s (contiguous vector get)", Arrays.toString(original)),
        expectedCombined,
        arr
    );

    // Check noncontiguous vector get.
    final int[] indexes = new int[expectedCombined.length];
    for (int i = 0; i < expectedCombined.length; i++) {
      indexes[indexes.length - 1 - i] = i; // Fetch backwards.
    }

    Arrays.fill(arr, Integer.MIN_VALUE);
    combined.get(arr, indexes, arr.length);
    final int[] expectedCombinedReversed = IntArrays.reverse(IntArrays.copy(expectedCombined));
    Assert.assertArrayEquals(
        StringUtils.format("%s (noncontiguous vector get, reversed)", Arrays.toString(original)),
        expectedCombinedReversed,
        arr
    );
  }
}
