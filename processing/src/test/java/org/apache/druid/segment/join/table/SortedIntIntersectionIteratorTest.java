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

package org.apache.druid.segment.join.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SortedIntIntersectionIteratorTest
{
  @Test
  public void test_iterator_allPossibleSingleListsWithCardinalityUpToThree()
  {
    // 8 possibilities
    for (int i = 0; i < 8; i++) {
      final IntList ints = intsFromBits(i);
      Assert.assertEquals(ints.toString(), ints, intersection(ints));
    }
  }

  @Test
  public void test_iterator_allPossibleSetsOfTwoListsWithCardinalityUpToSix()
  {
    // 4096 possibilities: 64 for each list, 2 lists
    for (int i = 0; i < 4096; i++) {
      final int bits1 = i & 63;
      final int bits2 = (i >> 6) & 63;

      final IntList ints1 = intsFromBits(bits1);
      final IntList ints2 = intsFromBits(bits2);

      Assert.assertEquals(
          StringUtils.format("ints1 = %s; ints2 = %s", ints1, ints2),
          intsFromBits(bits1 & bits2),
          intersection(ints1, ints2)
      );
    }
  }

  @Test
  public void test_iterator_allPossibleSetsOfThreeListsWithCardinalityUpToFour()
  {
    // 4096 possibilities: 16 for each list, 3 lists
    for (int i = 0; i < 4096; i++) {
      final int bits1 = i & 15;
      final int bits2 = (i >> 4) & 15;
      final int bits3 = (i >> 8) & 15;

      final IntList ints1 = intsFromBits(bits1);
      final IntList ints2 = intsFromBits(bits2);
      final IntList ints3 = intsFromBits(bits3);

      Assert.assertEquals(
          StringUtils.format("ints1 = %s; ints2 = %s; ints3 = %s", ints1, ints2, ints3),
          intsFromBits(bits1 & bits2 & bits3),
          intersection(ints1, ints2, ints3)
      );
    }
  }

  private static IntList intersection(final IntList... lists)
  {
    final SortedIntIntersectionIterator comboIterator = new SortedIntIntersectionIterator(
        Arrays.stream(lists)
              .map(IntList::iterator)
              .toArray(IntIterator[]::new)
    );

    return new IntArrayList(comboIterator);
  }

  private static IntList intsFromBits(final int bits)
  {
    final IntArrayList retVal = new IntArrayList(4);

    for (int i = 0; i < 32; i++) {
      if (((bits >> i) & 1) == 1) {
        retVal.add(i);
      }
    }

    return retVal;
  }
}
