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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.Utf8ValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

public final class IndexedUtf8ValueIndexes<TDictionary extends Indexed<ByteBuffer>>
    implements StringValueSetIndexes, Utf8ValueSetIndexes, ValueIndexes
{
  // This determines the cut-off point to switch the merging algorithm from doing binary-search per element in the value
  // set to doing a sorted merge algorithm between value set and dictionary. The ratio here represents the ratio b/w
  // the number of elements in value set and the number of elements in the dictionary. The number has been derived
  // using benchmark in https://github.com/apache/druid/pull/13133. If the ratio is higher than the threshold, we use
  // sorted merge instead of binary-search based algorithm.
  private static final double SORTED_MERGE_RATIO_THRESHOLD = 0.12D;
  private static final int SIZE_WORTH_CHECKING_MIN = 8;
  private static final Comparator<ByteBuffer> COMPARATOR = ByteBufferUtils.utf8Comparator();

  private final BitmapFactory bitmapFactory;
  private final TDictionary dictionary;
  private final Indexed<ImmutableBitmap> bitmaps;

  public IndexedUtf8ValueIndexes(
      BitmapFactory bitmapFactory,
      TDictionary dictionary,
      Indexed<ImmutableBitmap> bitmaps
  )
  {
    Preconditions.checkArgument(dictionary.isSorted(), "Dictionary must be sorted");
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
  }

  @Override
  public BitmapColumnIndex forValue(@Nullable String value)
  {
    final ByteBuffer utf8 = StringUtils.toUtf8ByteBuffer(value);
    return new SimpleBitmapColumnIndex()
    {

      @Override
      public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
      {
        if (includeUnknown && NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          return bitmapResultFactory.unionDimensionValueBitmaps(
              ImmutableList.of(getBitmapForValue(), getBitmap(0))
          );
        }
        return bitmapResultFactory.wrapDimensionValue(getBitmapForValue());
      }

      private ImmutableBitmap getBitmapForValue()
      {
        final int idx = dictionary.indexOf(utf8);
        return getBitmap(idx);
      }
    };
  }

  @Nullable
  @Override
  public BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType)
  {
    if (valueType.isPrimitive()) {
      return forValue(
          ExprEval.ofType(ExpressionType.fromColumnTypeStrict(valueType), value)
                  .castTo(ExpressionType.STRING)
                  .asString()
      );
    }
    return null;
  }

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  @Override
  public BitmapColumnIndex forSortedValues(SortedSet<String> values)
  {
    return getBitmapColumnIndexForSortedIterableUtf8(
        Iterables.transform(
            values,
            StringUtils::toUtf8ByteBuffer
        ),
        values.size(),
        values.contains(null)
    );
  }

  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  @Override
  public BitmapColumnIndex forSortedValuesUtf8(SortedSet<ByteBuffer> valuesUtf8)
  {
    final SortedSet<ByteBuffer> tailSet;

    if (valuesUtf8.size() >= SIZE_WORTH_CHECKING_MIN) {
      final ByteBuffer minValueInColumn = dictionary.get(0);
      tailSet = valuesUtf8.tailSet(minValueInColumn);
    } else {
      tailSet = valuesUtf8;
    }

    return getBitmapColumnIndexForSortedIterableUtf8(tailSet, tailSet.size(), valuesUtf8.contains(null));
  }

  private ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = bitmaps.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  /**
   * Helper used by {@link #forSortedValues} and {@link #forSortedValuesUtf8}.
   */
  private BitmapColumnIndex getBitmapColumnIndexForSortedIterableUtf8(Iterable<ByteBuffer> valuesUtf8, int size, boolean valuesContainsNull)
  {
    // for large number of in-filter values in comparison to the dictionary size, use the sorted merge algorithm.
    if (size > SORTED_MERGE_RATIO_THRESHOLD * dictionary.size()) {
      return new SimpleImmutableBitmapDelegatingIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final PeekingIterator<ByteBuffer> valuesIterator = Iterators.peekingIterator(valuesUtf8.iterator());
            final PeekingIterator<ByteBuffer> dictionaryIterator = Iterators.peekingIterator(dictionary.iterator());
            int next = -1;
            int idx = 0;

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
              while (next < 0 && valuesIterator.hasNext() && dictionaryIterator.hasNext()) {
                final ByteBuffer nextValue = valuesIterator.peek();
                final ByteBuffer nextDictionaryKey = dictionaryIterator.peek();
                final int comparison = COMPARATOR.compare(nextValue, nextDictionaryKey);
                if (comparison == 0) {
                  next = idx;
                  valuesIterator.next();
                  break;
                } else if (comparison < 0) {
                  valuesIterator.next();
                } else {
                  dictionaryIterator.next();
                  idx++;
                }
              }
            }
          };
        }

        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          if (!valuesContainsNull && NullHandling.isNullOrEquivalent(dictionary.get(0))) {
            return bitmaps.get(0);
          }
          return null;
        }
      };
    }

    // if the size of in-filter values is less than the threshold percentage of dictionary size, then use binary search
    // based lookup per value. The algorithm works well for smaller number of values.
    return getSimpleImmutableBitmapIterableIndexFromIterator(valuesUtf8, valuesContainsNull);
  }

  /**
   * Iterates over the value set, using binary search to look up each element. The algorithm works well for smaller
   * number of values, and must be used if the values are not sorted in the same manner as {@link #dictionary}
   */
  private SimpleImmutableBitmapDelegatingIterableIndex getSimpleImmutableBitmapIterableIndexFromIterator(Iterable<ByteBuffer> valuesUtf8, boolean valuesContainsNull)
  {
    return new SimpleImmutableBitmapDelegatingIterableIndex()
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          final int dictionarySize = dictionary.size();
          final Iterator<ByteBuffer> iterator = valuesUtf8.iterator();
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
              ByteBuffer nextValue = iterator.next();
              next = dictionary.indexOf(nextValue);

              if (next == -dictionarySize - 1) {
                // nextValue is past the end of the dictionary so we can break early
                // Note: we can rely on indexOf returning (-(insertion point) - 1), because of the earlier check
                // for Indexed.isSorted(), which guarantees this behavior
                break;
              }
            }
          }
        };
      }

      @Nullable
      @Override
      protected ImmutableBitmap getUnknownsBitmap()
      {
        if (!valuesContainsNull && NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          return bitmaps.get(0);
        }
        return null;
      }
    };
  }
}
