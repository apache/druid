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

package org.apache.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.collections.bitmap.BitSetBitmapFactory;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.segment.data.Offset;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
@RunWith(Parameterized.class)
public class BitmapOffsetTest
{
  private static final int[] TEST_VALS = {1, 2, 4, 291, 27412, 49120, 212312, 2412101};
  private static final int[] TEST_VALS_FLIP = {2412101, 212312, 49120, 27412, 291, 4, 2, 1};

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        Sets.cartesianProduct(
            ImmutableSet.of(new ConciseBitmapFactory(), new RoaringBitmapFactory(), new BitSetBitmapFactory()),
            ImmutableSet.of(false, true)
        ),
        new Function<List<?>, Object[]>()
        {
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private final BitmapFactory factory;
  private final boolean descending;

  public BitmapOffsetTest(BitmapFactory factory, boolean descending)
  {
    this.factory = factory;
    this.descending = descending;
  }

  @Test
  public void testSanity()
  {
    MutableBitmap mutable = factory.makeEmptyMutableBitmap();
    for (int val : TEST_VALS) {
      mutable.add(val);
    }

    ImmutableBitmap bitmap = factory.makeImmutableBitmap(mutable);
    final BitmapOffset offset = BitmapOffset.of(bitmap, descending, bitmap.size());
    final int[] expected = descending ? TEST_VALS_FLIP : TEST_VALS;

    int count = 0;
    while (offset.withinBounds()) {
      Assert.assertEquals(expected[count], offset.getOffset());

      int cloneCount = count;
      Offset clonedOffset = offset.clone();
      while (clonedOffset.withinBounds()) {
        Assert.assertEquals(expected[cloneCount], clonedOffset.getOffset());

        ++cloneCount;
        clonedOffset.increment();
      }

      ++count;
      offset.increment();
    }
    Assert.assertEquals(count, expected.length);
  }

  @Test
  public void testMarkResetToMark()
  {
    MutableBitmap mutable = factory.makeEmptyMutableBitmap();
    IntList rows = new IntArrayList();
    for (int i = 0; i < 1_000_000; i++) {
      if (ThreadLocalRandom.current().nextBoolean()) {
        rows.add(i);
        mutable.add(i);
      }
    }

    ImmutableBitmap bitmap = factory.makeImmutableBitmap(mutable);
    final BitmapOffset offset = BitmapOffset.of(bitmap, descending, bitmap.size());
    final IntList expected;
    if (descending) {
      expected = new IntArrayList(rows.size());
      for (int i = rows.size() - 1; i >= 0; i--) {
        expected.add(rows.getInt(i));
      }
    } else {
      expected = rows;
    }

    int count = 0;
    int mark = rows.size() / 2;
    while (offset.withinBounds()) {
      Assert.assertEquals(expected.getInt(count), offset.getOffset());

      if (count == mark) {
        offset.mark();
      }

      ++count;
      offset.increment();
    }

    offset.resetToMark();
    count = mark;
    while (offset.withinBounds()) {
      Assert.assertEquals(expected.getInt(count), offset.getOffset());

      if (count == mark) {
        offset.mark();
      }

      ++count;
      offset.increment();
    }

    Assert.assertEquals(count, expected.size());
  }

}
