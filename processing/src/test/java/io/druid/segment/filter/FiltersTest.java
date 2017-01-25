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

package io.druid.segment.filter;

import com.google.common.collect.Lists;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ConciseBitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.segment.IntIteratorUtils;
import io.druid.segment.column.BitmapIndex;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FiltersTest
{
  @Test
  public void testComputeNonOverlapRatioFromRandomBitmapSamplesWithFullyOverlappedBitmaps()
  {
    final int bitmapNum = 10;
    final List<ImmutableBitmap> bitmaps = Lists.newArrayListWithCapacity(bitmapNum);
    final BitmapIndex bitmapIndex = makeFullyOverlappedBitmapIndexes(bitmapNum, bitmaps);

    final double estimated = Filters.computeNonOverlapRatioFromRandomBitmapSamples(
        bitmapIndex,
        IntIteratorUtils.toIntList(IntIterators.fromTo(0, bitmapNum))
    );
    final double expected = 0.0;
    assertEquals(expected, estimated, 0.00001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComputeNonOverlapRatioFromRandomBitmapSamplesWithEmptyBitmaps()
  {
    final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
    final BitmapIndex bitmapIndex = getBitmapIndex(bitmaps);
    Filters.computeNonOverlapRatioFromRandomBitmapSamples(
        bitmapIndex,
        IntIteratorUtils.toIntList(IntIterators.EMPTY_ITERATOR)
    );
  }

  @Test
  public void testComputeNonOverlapRatioFromFirstNBitmapSamplesWithNonOverlapBitmaps() throws Exception
  {
    final int bitmapNum = 10;
    final List<ImmutableBitmap> bitmaps = Lists.newArrayListWithCapacity(bitmapNum);
    makeNonOverlappedBitmapIndexes(bitmapNum, bitmaps);

    final double estimated = Filters.computeNonOverlapRatioFromFirstNBitmapSamples(bitmaps);
    final double expected = 1.0;
    assertEquals(expected, estimated, 0.00001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComputeNonOverlapRatioFromFirstNBitmapSamplesWithEmptyBitmaps()
  {
    final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
    Filters.computeNonOverlapRatioFromFirstNBitmapSamples(bitmaps);
  }

  @Test
  public void testComputeNonOverlapRatioFromFirstNBitmapSamples() throws Exception
  {
    final int bitmapNum = Filters.SAMPLE_NUM_FOR_SELECTIVITY_ESTIMATION;
    final List<ImmutableBitmap> bitmaps = Lists.newArrayListWithCapacity(bitmapNum);
    makePartiallyOverlappedBitmapIndexes(bitmapNum, bitmaps);

    final double estimated = Filters.computeNonOverlapRatioFromFirstNBitmapSamples(bitmaps);
    final double expected = 0.2;
    assertEquals(expected, estimated, 0.00001);
  }

  @Test
  public void testEstimateSelectivityOfBitmapList()
  {
    final int bitmapNum = Filters.SAMPLE_NUM_FOR_SELECTIVITY_ESTIMATION;
    final List<ImmutableBitmap> bitmaps = Lists.newArrayListWithCapacity(bitmapNum);
    final BitmapIndex bitmapIndex = makePartiallyOverlappedBitmapIndexes(bitmapNum, bitmaps);

    final double estimated = Filters.estimateSelectivityOfBitmapList(
        bitmapIndex,
        IntIteratorUtils.toIntList(IntIterators.fromTo(0, bitmapNum)),
        1000,
        true
    );
    final double expected = 0.208; // total # of bits is 208 = 10 + 99 * 2
    assertEquals(expected, estimated, 0.00001);
  }

  @Test
  public void testEstimateSelectivityOfBitmapTree()
  {
    final int bitmapNum = Filters.SAMPLE_NUM_FOR_SELECTIVITY_ESTIMATION;
    final List<ImmutableBitmap> bitmaps = Lists.newArrayListWithCapacity(bitmapNum);
    makePartiallyOverlappedBitmapIndexes(bitmapNum, bitmaps);

    final double estimated = Filters.estimateSelectivityOfBitmapTree(
        bitmaps,
        1000,
        true
    );
    final double expected = 0.208; // total # of bits is 208 = 10 + 99 * 2
    assertEquals(expected, estimated, 0.00001);
  }

  private static BitmapIndex getBitmapIndex(final List<ImmutableBitmap> bitmapList)
  {
    return new BitmapIndex()
    {
      @Override
      public int getCardinality()
      {
        return 10;
      }

      @Override
      public String getValue(int index)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasNulls()
      {
        return false;
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return new ConciseBitmapFactory();
      }

      @Override
      public int getIndex(String value)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ImmutableBitmap getBitmap(int idx)
      {
        return bitmapList.get(idx);
      }
    };
  }

  private static BitmapIndex makeFullyOverlappedBitmapIndexes(final int bitmapNum, final List<ImmutableBitmap> bitmaps)
  {
    final BitmapIndex bitmapIndex = getBitmapIndex(bitmaps);
    final BitmapFactory factory = bitmapIndex.getBitmapFactory();
    for (int i = 0; i < bitmapNum; i++) {
      final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
      for (int j = 0; j < 10; j++) {
        mutableBitmap.add(j * 10);
      }
      bitmaps.add(factory.makeImmutableBitmap(mutableBitmap));
    }
    return bitmapIndex;
  }

  private static BitmapIndex makeNonOverlappedBitmapIndexes(final int bitmapNum, final List<ImmutableBitmap> bitmaps)
  {
    final BitmapIndex bitmapIndex = getBitmapIndex(bitmaps);
    final BitmapFactory factory = bitmapIndex.getBitmapFactory();
    int index = 0;
    for (int i = 0; i < bitmapNum; i++) {
      final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
      for (int j = 0; j < 10; j++) {
        mutableBitmap.add(index++);
      }
      bitmaps.add(factory.makeImmutableBitmap(mutableBitmap));
    }
    return bitmapIndex;
  }

  private static BitmapIndex makePartiallyOverlappedBitmapIndexes(int bitmapNum, List<ImmutableBitmap> bitmaps)
  {
    final BitmapIndex bitmapIndex = getBitmapIndex(bitmaps);
    final BitmapFactory factory = bitmapIndex.getBitmapFactory();
    int startIndex = 0;
    for (int i = 0; i < bitmapNum; i++) {
      final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
      for (int j = 0; j < 10; j++) {
        mutableBitmap.add(startIndex + j);
      }
      startIndex += 2; // 80% of bitmaps are overlapped
      bitmaps.add(factory.makeImmutableBitmap(mutableBitmap));
    }
    return bitmapIndex;
  }
}
