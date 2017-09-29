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
import io.druid.segment.data.ReadableOffset;
import io.druid.segment.filter.BooleanValueMatcher;
import org.roaringbitmap.IntIterator;

public final class FilteredOffset extends Offset
{
  private final Offset baseOffset;
  private final ValueMatcher filterMatcher;

  FilteredOffset(
      Offset baseOffset,
      ColumnSelectorFactory columnSelectorFactory,
      boolean descending,
      Filter postFilter,
      ColumnSelectorBitmapIndexSelector bitmapIndexSelector
  )
  {
    this.baseOffset = baseOffset;
    RowOffsetMatcherFactory rowOffsetMatcherFactory = new CursorOffsetHolderRowOffsetMatcherFactory(
        baseOffset.getBaseReadableOffset(),
        descending
    );
    if (postFilter instanceof BooleanFilter) {
      filterMatcher = ((BooleanFilter) postFilter).makeMatcher(
          bitmapIndexSelector,
          columnSelectorFactory,
          rowOffsetMatcherFactory
      );
    } else {
      if (postFilter.supportsBitmapIndex(bitmapIndexSelector)) {
        filterMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(
            postFilter.getBitmapIndex(bitmapIndexSelector)
        );
      } else {
        filterMatcher = postFilter.makeMatcher(columnSelectorFactory);
      }
    }
    incrementIfNeededOnCreationOrReset();
  }

  @Override
  public void increment()
  {
    while (!Thread.currentThread().isInterrupted()) {
      baseOffset.increment();
      if (!baseOffset.withinBounds() || filterMatcher.matches()) {
        return;
      }
    }
  }

  @Override
  public boolean withinBounds()
  {
    return baseOffset.withinBounds();
  }

  @Override
  public void reset()
  {
    baseOffset.reset();
    incrementIfNeededOnCreationOrReset();
  }

  private void incrementIfNeededOnCreationOrReset()
  {
    if (baseOffset.withinBounds()) {
      if (!filterMatcher.matches()) {
        increment();
        // increment() returns early if it detects the current Thread is interrupted. It will leave this
        // FilteredOffset in an illegal state, because it may point to an offset that should be filtered. So must to
        // call BaseQuery.checkInterrupted() and thereby throw a QueryInterruptedException.
        BaseQuery.checkInterrupted();
      }
    }
  }

  @Override
  public ReadableOffset getBaseReadableOffset()
  {
    return baseOffset.getBaseReadableOffset();
  }

  @Override
  public Offset clone()
  {
    throw new UnsupportedOperationException("FilteredOffset should not be cloned");
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
    private final ReadableOffset offset;
    private final boolean descending;

    CursorOffsetHolderRowOffsetMatcherFactory(ReadableOffset offset, boolean descending)
    {
      this.offset = offset;
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
            int currentOffset = offset.getOffset();
            while (iterOffset > currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("offset", offset);
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
            int currentOffset = offset.getOffset();
            while (iterOffset < currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("offset", offset);
            inspector.visit("iter", iter);
          }
        };
      }
    }
  }
}
