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

import com.google.common.base.Supplier;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Provides {@link BitmapIndex} for some dictionary encoded column, where the dictionary and bitmaps are stored in some
 * {@link GenericIndexed}.
 */
public class StringBitmapIndexColumnPartSupplier implements Supplier<BitmapIndex>
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final GenericIndexed<String> dictionary;

  public StringBitmapIndexColumnPartSupplier(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      GenericIndexed<String> dictionary
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
  }

  @Override
  public BitmapIndex get()
  {
    return new BitmapIndex()
    {
      @Override
      public int getCardinality()
      {
        return dictionary.size();
      }

      @Override
      public String getValue(int index)
      {
        return dictionary.get(index);
      }

      @Override
      public boolean hasNulls()
      {
        return dictionary.indexOf(null) >= 0;
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return bitmapFactory;
      }

      @Override
      public int getIndex(@Nullable String value)
      {
        return dictionary.indexOf(value);
      }

      @Override
      public ImmutableBitmap getBitmap(int idx)
      {
        if (idx < 0) {
          return bitmapFactory.makeEmptyImmutableBitmap();
        }

        final ImmutableBitmap bitmap = bitmaps.get(idx);
        return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
      }

      @Override
      public ImmutableBitmap getBitmapForValue(@Nullable String value)
      {
        final int idx = dictionary.indexOf(value);
        return getBitmap(idx);
      }

      @Override
      public Iterable<ImmutableBitmap> getBitmapsInRange(
          @Nullable String startValue,
          boolean startStrict,
          @Nullable String endValue,
          boolean endStrict
      )
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

      @Override
      public Iterable<ImmutableBitmap> getBitmapsInRange(
          @Nullable String startValue,
          boolean startStrict,
          @Nullable String endValue,
          boolean endStrict,
          Predicate<String> indexMatcher
      )
      {
        final IntIntPair range = getRange(startValue, startStrict, endValue, endStrict);
        final int start = range.leftInt(), end = range.rightInt();
        return () -> new Iterator<ImmutableBitmap>()
        {
          int currIndex = start;
          int found;
          {
            found = findNext();
          }

          private int findNext()
          {
            while (currIndex < end && !indexMatcher.test(dictionary.get(currIndex))) {
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

      @Override
      public Iterable<ImmutableBitmap> getBitmapsForValues(Set<String> values)
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          final Iterator<String> iterator = values.iterator();
          int next = -1;

          @Override
          public boolean hasNext()
          {
            if (next < 0) {
              findNext();
            }
            return next >= 0;
          }

          @Override
          public ImmutableBitmap next()
          {
            if (next < 0) {
              findNext();
              if (next < 0) {
                throw new NoSuchElementException();
              }
            }
            final int swap = next;
            next = -1;
            return getBitmap(swap);
          }

          private void findNext()
          {
            while (next < 0 && iterator.hasNext()) {
              String nextValue = iterator.next();
              next = dictionary.indexOf(nextValue);
            }
          }
        };
      }

      private IntIntPair getRange(@Nullable String startValue, boolean startStrict, @Nullable String endValue, boolean endStrict)
      {
        int startIndex, endIndex;
        if (startValue == null) {
          startIndex = 0;
        } else {
          final int found = dictionary.indexOf(NullHandling.emptyToNullIfNeeded(startValue));
          if (found >= 0) {
            startIndex = startStrict ? found + 1 : found;
          } else {
            startIndex = -(found + 1);
          }
        }

        if (endValue == null) {
          endIndex = dictionary.size();
        } else {
          final int found = dictionary.indexOf(NullHandling.emptyToNullIfNeeded(endValue));
          if (found >= 0) {
            endIndex = endStrict ? found : found + 1;
          } else {
            endIndex = -(found + 1);
          }
        }

        endIndex = Math.max(startIndex, endIndex);
        return new IntIntImmutablePair(startIndex, endIndex);
      }
    };
  }
}
