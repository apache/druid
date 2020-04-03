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

package org.apache.druid.segment.filter;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.IntIteratorUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FiltersTest extends InitializedNullHandlingTest
{
  @Test
  public void testEstimateSelectivityOfBitmapList()
  {
    final int bitmapNum = 100;
    final List<ImmutableBitmap> bitmaps = Lists.newArrayListWithCapacity(bitmapNum);
    final BitmapIndex bitmapIndex = makeNonOverlappedBitmapIndexes(bitmapNum, bitmaps);

    final double estimated = Filters.estimateSelectivity(
        bitmapIndex,
        IntIteratorUtils.toIntList(IntIterators.fromTo(0, bitmapNum)),
        10000
    );
    final double expected = 0.1;
    Assert.assertEquals(expected, estimated, 0.00001);
  }

  @Test
  public void testPushDownNot()
  {
    final Filter filter = FilterTestUtils.not(
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "1"),
            FilterTestUtils.selector("col2", "2"),
            FilterTestUtils.not(FilterTestUtils.selector("col3", "3"))
        )
    );
    final Filter expected = FilterTestUtils.or(
        FilterTestUtils.not(FilterTestUtils.selector("col1", "1")),
        FilterTestUtils.not(FilterTestUtils.selector("col2", "2")),
        FilterTestUtils.selector("col3", "3")
    );
    Assert.assertEquals(expected, Filters.pushDownNot(filter));
  }

  @Test
  public void testPushDownNotLeafNot()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "1"),
        FilterTestUtils.selector("col2", "2"),
        FilterTestUtils.not(FilterTestUtils.selector("col3", "3"))
    );
    Assert.assertEquals(filter, Filters.pushDownNot(filter));
  }

  @Test
  public void testFlatten()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.and(
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "1"),
                FilterTestUtils.selector("col2", "2")
            )
        ),
        FilterTestUtils.selector("col3", "3")
    );
    final Filter expected = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "1"),
        FilterTestUtils.selector("col2", "2"),
        FilterTestUtils.selector("col3", "3")
    );
    Assert.assertEquals(expected, Filters.flatten(filter));
  }

  @Test
  public void testFlattenUnflattenable()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "1"),
            FilterTestUtils.selector("col2", "2")
        ),
        FilterTestUtils.selector("col3", "3")
    );
    Assert.assertEquals(filter, Filters.flatten(filter));
  }

  @Test
  public void testToCNFWithMuchReducibleFilter()
  {
    final Filter muchReducible = FilterTestUtils.and(
        // should be flattened
        FilterTestUtils.and(
            FilterTestUtils.and(
                FilterTestUtils.and(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should be flattened
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.and(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should be flattened
        FilterTestUtils.or(
            FilterTestUtils.and(
                FilterTestUtils.or(FilterTestUtils.selector("col1", "val1"))
            )
        ),
        // should eliminate duplicate filters
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2"),
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val2")
        ),
        FilterTestUtils.and(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.and(
                FilterTestUtils.selector("col2", "val2"),
                FilterTestUtils.selector("col1", "val1")
            )
        )
    );
    final Filter expected = FilterTestUtils.and(
        FilterTestUtils.selector("col1", "val1"),
        FilterTestUtils.selector("col2", "val2")
    );
    Assert.assertEquals(expected, Filters.toCNF(muchReducible));
  }

  @Test
  public void testToCNFWithComplexFilterIncludingNotAndOr()
  {
    final Filter filter = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val2")
            ),
            FilterTestUtils.not(
                FilterTestUtils.and(
                    FilterTestUtils.selector("col4", "val4"),
                    FilterTestUtils.selector("col5", "val5")
                )
            )
        ),
        FilterTestUtils.or(
            FilterTestUtils.not(
                FilterTestUtils.or(
                    FilterTestUtils.selector("col2", "val2"),
                    FilterTestUtils.selector("col4", "val4"),
                    FilterTestUtils.selector("col5", "val5")
                )
            ),
            FilterTestUtils.and(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col3", "val3")
            )
        ),
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val22"), // selecting different value
                FilterTestUtils.selector("col3", "val3")
            ),
            FilterTestUtils.not(
                FilterTestUtils.selector("col1", "val11")
            )
        ),
        FilterTestUtils.and(
            FilterTestUtils.or(
                FilterTestUtils.selector("col1", "val1"),
                FilterTestUtils.selector("col2", "val22"),
                FilterTestUtils.selector("col3", "val3")
            ),
            FilterTestUtils.not(
                FilterTestUtils.selector("col1", "val11") // selecting different value
            )
        )
    );
    final Filter expected = FilterTestUtils.and(
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.selector("col2", "val22"),
            FilterTestUtils.selector("col3", "val3")
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col2", "val2"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.not(FilterTestUtils.selector("col2", "val2")),
            FilterTestUtils.selector("col3", "val3")
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col3", "val3"),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.not(FilterTestUtils.selector("col1", "val11")),
        // The below OR filter could be eliminated because this filter also has
        // (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)).
        // The reduction process would be
        // (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)) && (col1 = val1 || ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 && ~(col4 = val4) || ~(col5 = val5)) && (col1 = val1 || ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 && ~(col4 = val4) || ~(col5 = val5))
        // => (col1 = val1 || ~(col4 = val4)) && (col1 = val1 || ~(col5 = val5)).
        // However, we don't have this reduction now, so we have a filter in a suboptimized CNF.
        FilterTestUtils.or(
            FilterTestUtils.selector("col1", "val1"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4")),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        ),
        FilterTestUtils.or(
            FilterTestUtils.selector("col2", "val2"),
            FilterTestUtils.not(FilterTestUtils.selector("col4", "val4")),
            FilterTestUtils.not(FilterTestUtils.selector("col5", "val5"))
        )
    );
    Assert.assertEquals(expected, Filters.toCNF(filter));
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

  private static BitmapIndex makeNonOverlappedBitmapIndexes(final int bitmapNum, final List<ImmutableBitmap> bitmaps)
  {
    final BitmapIndex bitmapIndex = getBitmapIndex(bitmaps);
    final BitmapFactory factory = bitmapIndex.getBitmapFactory();
    for (int i = 0; i < bitmapNum; i++) {
      final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
      for (int j = 0; j < 10; j++) {
        mutableBitmap.add(i * 10 + j);
      }
      bitmaps.add(factory.makeImmutableBitmap(mutableBitmap));
    }
    return bitmapIndex;
  }
}
