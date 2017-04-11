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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.collections.bitmap.BitSetBitmapFactory;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ConciseBitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.segment.data.Offset;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class BitmapOffsetTest
{
  private static final int[] TEST_VALS = {1, 2, 4, 291, 27412, 49120, 212312, 2412101};
  private static final int[] TEST_VALS_FLIP = {2412101, 212312, 49120, 27412, 291, 4, 2, 1};

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
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
  public void testSanity() throws Exception
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
}
