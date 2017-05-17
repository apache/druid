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

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.BaseQuery;
import io.druid.query.filter.BooleanFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.RowOffsetMatcherFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.historical.HistoricalCursor;
import io.druid.segment.historical.OffsetHolder;
import org.roaringbitmap.IntIterator;

public final class FilteredOffset extends Offset
{
  private Offset baseOffset;
  private final ValueMatcher filterMatcher;

  FilteredOffset(
      HistoricalCursor cursor,
      boolean descending,
      Filter postFilter,
      ColumnSelectorBitmapIndexSelector bitmapIndexSelector
  )
  {
    RowOffsetMatcherFactory rowOffsetMatcherFactory = new CursorOffsetHolderRowOffsetMatcherFactory(
        cursor,
        descending
    );
    if (postFilter instanceof BooleanFilter) {
      filterMatcher = ((BooleanFilter) postFilter).makeMatcher(
          bitmapIndexSelector,
          cursor,
          rowOffsetMatcherFactory
      );
    } else {
      if (postFilter.supportsBitmapIndex(bitmapIndexSelector)) {
        filterMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(
            postFilter.getBitmapIndex(bitmapIndexSelector)
        );
      } else {
        filterMatcher = postFilter.makeMatcher(cursor);
      }
    }
  }

  void reset(Offset baseOffset)
  {
    this.baseOffset = baseOffset;
    if (baseOffset.withinBounds()) {
      if (!filterMatcher.matches()) {
        BaseQuery.checkInterrupted();
        incrementInterruptibly();
      }
    }
  }

  @Override
  public void increment()
  {
    baseOffset.increment();

    while (baseOffset.withinBounds() && !Thread.currentThread().isInterrupted()) {
      if (filterMatcher.matches()) {
        return;
      } else {
        baseOffset.increment();
      }
    }
  }

  void incrementInterruptibly()
  {
    baseOffset.increment();
    while (baseOffset.withinBounds()) {
      BaseQuery.checkInterrupted();
      if (filterMatcher.matches()) {
        return;
      } else {
        baseOffset.increment();
      }
    }
  }

  @Override
  public boolean withinBounds()
  {
    return baseOffset.withinBounds();
  }

  @Override
  public Offset clone()
  {
    FilteredOffset offset = (FilteredOffset) super.clone();
    offset.baseOffset = offset.baseOffset.clone();
    return offset;
  }

  @Override
  public int getOffset()
  {
    return baseOffset.getOffset();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("baseOffset", baseOffset);
    inspector.visit("filterMatcher", filterMatcher);
  }

  private static class CursorOffsetHolderRowOffsetMatcherFactory implements RowOffsetMatcherFactory
  {
    private final OffsetHolder holder;
    private final boolean descending;

    CursorOffsetHolderRowOffsetMatcherFactory(OffsetHolder holder, boolean descending)
    {
      this.holder = holder;
      this.descending = descending;
    }

    // Use an iterator-based implementation, ImmutableBitmap.get(index) works differently for Concise and Roaring.
    // ImmutableConciseSet.get(index) is also inefficient, it performs a linear scan on each call
    @Override
    public ValueMatcher makeRowOffsetMatcher(final ImmutableBitmap rowBitmap)
    {
      final IntIterator iter = descending ?
                               BitmapOffset.getReverseBitmapOffsetIterator(rowBitmap) :
                               rowBitmap.iterator();

      if (!iter.hasNext()) {
        return BooleanValueMatcher.of(false);
      }

      if (descending) {
        return new ValueMatcher()
        {
          int iterOffset = Integer.MAX_VALUE;

          @Override
          public boolean matches()
          {
            int currentOffset = holder.getOffset().getOffset();
            while (iterOffset > currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("holder", holder);
            inspector.visit("iter", iter);
          }
        };
      } else {
        return new ValueMatcher()
        {
          int iterOffset = -1;

          @Override
          public boolean matches()
          {
            int currentOffset = holder.getOffset().getOffset();
            while (iterOffset < currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("holder", holder);
            inspector.visit("iter", iter);
          }
        };
      }
    }
  }
}
