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

package org.apache.druid.segment.index;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class IndexedUtf8LexicographicalRangeIndexes<TDictionary extends Indexed<ByteBuffer>>
    implements LexicographicalRangeIndexes
{
  private final BitmapFactory bitmapFactory;
  private final TDictionary dictionary;
  private final Indexed<ImmutableBitmap> bitmaps;
  private final boolean hasNull;

  private final ColumnConfig columnConfig;
  private final int numRows;

  public IndexedUtf8LexicographicalRangeIndexes(
      BitmapFactory bitmapFactory,
      TDictionary dictionary,
      Indexed<ImmutableBitmap> bitmaps,
      boolean hasNull,
      @Nullable ColumnConfig columnConfig,
      int numRows
  )
  {
    Preconditions.checkArgument(dictionary.isSorted(), "Dictionary must be sorted");
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
    this.hasNull = hasNull;
    this.columnConfig = columnConfig;
    this.numRows = numRows;
  }

  @Override
  @Nullable
  public BitmapColumnIndex forRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict
  )
  {
    final IntIntPair range = getRange(startValue, startStrict, endValue, endStrict);
    final int start = range.leftInt(), end = range.rightInt();
    if (ColumnIndexSupplier.skipComputingRangeIndexes(columnConfig, numRows, end - start)) {
      return null;
    }
    return new SimpleImmutableBitmapDelegatingIterableIndex()
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        final IntIntPair range = getRange(startValue, startStrict, endValue, endStrict);
        final int start = range.leftInt(), end = range.rightInt();
        return () -> new Iterator<ImmutableBitmap>()
        {
          final IntIterator rangeIterator = IntListUtils.fromTo(start, end).iterator();

          @Override
          public boolean hasNext()
          {
            return rangeIterator.hasNext();
          }

          @Override
          public ImmutableBitmap next()
          {
            return getBitmap(rangeIterator.nextInt());
          }
        };
      }

      @Nullable
      @Override
      protected ImmutableBitmap getUnknownsBitmap()
      {
        if (NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          return bitmaps.get(0);
        }
        return null;
      }
    };
  }

  @Override
  @Nullable
  public BitmapColumnIndex forRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict,
      DruidObjectPredicate<String> matcher
  )
  {
    final IntIntPair range = getRange(startValue, startStrict, endValue, endStrict);
    final int start = range.leftInt(), end = range.rightInt();
    if (ColumnIndexSupplier.skipComputingRangeIndexes(columnConfig, numRows, end - start)) {
      return null;
    }
    return new SimpleImmutableBitmapDelegatingIterableIndex()
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          int currIndex = start;
          int found;

          {
            found = findNext();
          }

          private int findNext()
          {
            while (currIndex < end && !applyMatcher(dictionary.get(currIndex)).matches(false)) {
              currIndex++;
            }

            if (currIndex < end) {
              return currIndex++;
            } else {
              return -1;
            }
          }

          @Override
          public boolean hasNext()
          {
            return found != -1;
          }

          @Override
          public ImmutableBitmap next()
          {
            int cur = found;

            if (cur == -1) {
              throw new NoSuchElementException();
            }

            found = findNext();
            return getBitmap(cur);
          }
        };
      }

      @Nullable
      @Override
      protected ImmutableBitmap getUnknownsBitmap()
      {
        if (NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          return bitmaps.get(0);
        }
        return null;
      }

      private DruidPredicateMatch applyMatcher(@Nullable final ByteBuffer valueUtf8)
      {
        if (valueUtf8 == null) {
          return matcher.apply(null);
        } else {
          // Duplicate buffer, because StringUtils.fromUtf8 advances the position, and we do not want to do that.
          return matcher.apply(StringUtils.fromUtf8(valueUtf8.duplicate()));
        }
      }
    };
  }

  private IntIntPair getRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict
  )
  {
    final int firstValue = hasNull ? 1 : 0;
    int startIndex, endIndex;
    if (startValue == null) {
      startIndex = firstValue;
    } else {
      final String startValueToUse = NullHandling.emptyToNullIfNeeded(startValue);
      final int found = dictionary.indexOf(StringUtils.toUtf8ByteBuffer(startValueToUse));
      if (found >= firstValue) {
        startIndex = startStrict ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (endValue == null) {
      endIndex = dictionary.size();
    } else {
      final String endValueToUse = NullHandling.emptyToNullIfNeeded(endValue);
      final int found = dictionary.indexOf(StringUtils.toUtf8ByteBuffer(endValueToUse));
      if (found >= firstValue) {
        endIndex = endStrict ? found : found + 1;
      } else {
        endIndex = -(found + 1);
      }
    }

    endIndex = Math.max(startIndex, endIndex);
    return new IntIntImmutablePair(startIndex, endIndex);
  }

  private ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = bitmaps.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }
}
