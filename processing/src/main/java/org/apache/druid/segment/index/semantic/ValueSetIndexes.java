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

package org.apache.druid.segment.index.semantic;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapDelegatingIterableIndex;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Construct a {@link BitmapColumnIndex} for a set of values which might be present in the column.
 */
public interface ValueSetIndexes
{
  /**
   * threshold of sorted match value iterator size compared to dictionary size to use
   * {@link #buildBitmapColumnIndexFromSortedIteratorScan} instead of
   * {@link #buildBitmapColumnIndexFromSortedIteratorBinarySearch}.
   */
  double SORTED_SCAN_RATIO_THRESHOLD = 0.12D;

  /**
   * minimum sorted match value iterator size to trim the initial values from the iterator to seek to the start of the
   * value dictionary when using {@link #buildBitmapColumnIndexFromSortedIteratorScan} or
   * {@link #buildBitmapColumnIndexFromSortedIteratorBinarySearch}.
   */
  int SIZE_WORTH_CHECKING_MIN = 8;

  /**
   * Get the wrapped {@link ImmutableBitmap} corresponding to the specified set of values (if they are contained in the
   * underlying column). The set must be sorted using the comparator of the supplied matchValueType.
   *
   * @param sortedValues   values to match, sorted in matchValueType order
   * @param matchValueType type of the value to match, used to assist conversion from the match value type to the column
   *                       value type
   * @return               {@link ImmutableBitmap} corresponding to the rows which match the values, or null if an index
   *                       connot be computed for the supplied value type
   */
  @Nullable
  BitmapColumnIndex forSortedValues(@Nonnull List<?> sortedValues, TypeSignature<ValueType> matchValueType);


  /**
   * Helper method for implementing {@link #forSortedValues} for a value set that is sorted the same as the column
   * dictionary.
   * <p>
   * Builds a {@link BitmapColumnIndex} from an {@link Iterable<T>} that is sorted the same as the columns
   * {@link Indexed<T>} value dictionary. Uses a strategy that does zipping similar to the merge step of a sort-merge,
   * where we step forward on both the iterator and the dictionary to find matches to build a
   * {@link Iterable<ImmutableBitmap>}.
   * <p> 
   * If sorted match value iterator size is greater than (dictionary size * {@link #SORTED_SCAN_RATIO_THRESHOLD}),
   * consider using this method instead of {@link #buildBitmapColumnIndexFromSortedIteratorBinarySearch}.
   * <p> 
   * If the values in the iterator are NOT sorted the same as the dictionary, do NOT use this method, use
   * {@link #buildBitmapColumnIndexFromIteratorBinarySearch} instead.
   */
  static <T> BitmapColumnIndex buildBitmapColumnIndexFromSortedIteratorScan(
      BitmapFactory bitmapFactory,
      Comparator<T> comparator,
      Iterable<T> values,
      Indexed<T> dictionary,
      Indexed<ImmutableBitmap> bitmaps,
      Supplier<ImmutableBitmap> unknownsBitmap
  )
  {
    return new BaseValueSetIndexesFromIterable(bitmapFactory, bitmaps, unknownsBitmap)
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          final PeekingIterator<T> valuesIterator = Iterators.peekingIterator(values.iterator());
          final PeekingIterator<T> dictionaryIterator = Iterators.peekingIterator(dictionary.iterator());
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
              final T nextValue = valuesIterator.peek();
              final T nextDictionaryKey = dictionaryIterator.peek();
              final int comparison = comparator.compare(nextValue, nextDictionaryKey);
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
    };
  }

  /**
   * Helper method for implementing {@link #forSortedValues} for a value set that is sorted the same as the column
   * dictionary.
   * <p>
   * Builds a {@link BitmapColumnIndex} from an {@link Iterable<T>} that is sorted the same as the columns
   * {@link Indexed<T>} value dictionary. This algorithm iterates the values to match and does a binary search for
   * matching values using {@link Indexed#indexOf(Object)} to build a {@link Iterable<ImmutableBitmap>} short-circuiting
   * the iteration if we reach the end of the {@link Indexed} before the values to match are exhausted.
   * <p>
   * If sorted match value iterator size is less than (dictionary size * {@link #SORTED_SCAN_RATIO_THRESHOLD}),
   * consider using this method instead of {@link #buildBitmapColumnIndexFromSortedIteratorScan}.
   * <p> 
   * If the values in the iterator are not sorted the same as the dictionary, do not use this method, use
   * {@link #buildBitmapColumnIndexFromIteratorBinarySearch} instead.
   */
  static <T> BitmapColumnIndex buildBitmapColumnIndexFromSortedIteratorBinarySearch(
      BitmapFactory bitmapFactory,
      Iterable<T> values,
      Indexed<T> dictionary,
      Indexed<ImmutableBitmap> bitmaps,
      Supplier<ImmutableBitmap> getUnknownsIndex
  )
  {
    return new BaseValueSetIndexesFromIterable(bitmapFactory, bitmaps, getUnknownsIndex)
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          final int dictionarySize = dictionary.size();
          final Iterator<T> iterator = values.iterator();
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
              T nextValue = iterator.next();
              next = dictionary.indexOf(nextValue);

              if (next == -dictionarySize - 1) {
                // nextValue is past the end of the dictionary so we can break early
                // Note: we can rely on indexOf returning (-(insertion point) - 1), because the Indexed
                // is sorted, which guarantees this behavior
                break;
              }
            }
          }
        };
      }
    };
  }

  /**
   * Helper method for implementing {@link #forSortedValues} for a value set that is NOT sorted the same as the column
   * dictionary.
   * <p>
   * Builds a {@link BitmapColumnIndex} from an {@link Iterable<T>} that is NOT sorted the same as the columns
   * {@link Indexed<T>} value dictionary. This algorithm iterates the values to match and does a binary search for
   * matching values using {@link Indexed#indexOf(Object)} to build a {@link Iterable<ImmutableBitmap>} until the match
   * values iterator is exhausted.
   * <p> 
   * If values of the iterator are sorted the same as the dictionary, use
   * {@link #buildBitmapColumnIndexFromSortedIteratorScan} or
   * {@link #buildBitmapColumnIndexFromSortedIteratorBinarySearch} instead.
   */
  static <T> BitmapColumnIndex buildBitmapColumnIndexFromIteratorBinarySearch(
      BitmapFactory bitmapFactory,
      Iterable<T> values,
      Indexed<T> dictionary,
      Indexed<ImmutableBitmap> bitmaps,
      Supplier<ImmutableBitmap> getUnknownsIndex
  )
  {
    return new BaseValueSetIndexesFromIterable(bitmapFactory, bitmaps, getUnknownsIndex)
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          final Iterator<T> iterator = values.iterator();
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
              T nextValue = iterator.next();
              next = dictionary.indexOf(nextValue);
            }
          }
        };
      }
    };
  }

  abstract class BaseValueSetIndexesFromIterable extends SimpleImmutableBitmapDelegatingIterableIndex
  {
    private final Indexed<ImmutableBitmap> bitmaps;
    private final BitmapFactory bitmapFactory;
    private final Supplier<ImmutableBitmap> unknownsBitmap;

    public BaseValueSetIndexesFromIterable(
        BitmapFactory bitmapFactory,
        Indexed<ImmutableBitmap> bitmaps,
        Supplier<ImmutableBitmap> unknownsBitmap
    )
    {
      this.bitmaps = bitmaps;
      this.bitmapFactory = bitmapFactory;
      this.unknownsBitmap = unknownsBitmap;
    }

    @Nullable
    @Override
    protected ImmutableBitmap getUnknownsBitmap()
    {
      return unknownsBitmap.get();
    }

    protected ImmutableBitmap getBitmap(int idx)
    {
      if (idx < 0) {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }

      final ImmutableBitmap bitmap = bitmaps.get(idx);
      return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
    }
  }
}
