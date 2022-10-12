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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.SimpleBitmapColumnIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.column.SpatialIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.column.Utf8ValueSetIndex;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

public class DictionaryEncodedStringIndexSupplier implements ColumnIndexSupplier
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<String> dictionary;
  private final GenericIndexed<ByteBuffer> dictionaryUtf8;
  @Nullable
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  @Nullable
  private final ImmutableRTree indexedTree;

  public DictionaryEncodedStringIndexSupplier(
      BitmapFactory bitmapFactory,
      GenericIndexed<String> dictionary,
      GenericIndexed<ByteBuffer> dictionaryUtf8,
      @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
      @Nullable ImmutableRTree indexedTree
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.dictionaryUtf8 = dictionaryUtf8;
    this.bitmaps = bitmaps;
    this.indexedTree = indexedTree;
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (bitmaps != null) {
      if (clazz.equals(NullValueIndex.class)) {
        final BitmapColumnIndex nullIndex;
        if (NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          nullIndex = new SimpleImmutableBitmapIndex(bitmaps.get(0));
        } else {
          nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
        }
        return (T) (NullValueIndex) () -> nullIndex;
      } else if (clazz.equals(StringValueSetIndex.class)) {
        return (T) new GenericIndexedDictionaryEncodedStringValueSetIndex(bitmapFactory, dictionaryUtf8, bitmaps);
      } else if (clazz.equals(Utf8ValueSetIndex.class)) {
        return (T) new GenericIndexedDictionaryEncodedStringValueSetIndex(bitmapFactory, dictionaryUtf8, bitmaps);
      } else if (clazz.equals(DruidPredicateIndex.class)) {
        return (T) new GenericIndexedDictionaryEncodedStringDruidPredicateIndex(bitmapFactory, dictionary, bitmaps);
      } else if (clazz.equals(LexicographicalRangeIndex.class)) {
        return (T) new GenericIndexedDictionaryEncodedColumnLexicographicalRangeIndex(
            bitmapFactory,
            dictionaryUtf8,
            bitmaps,
            NullHandling.isNullOrEquivalent(dictionary.get(0))
        );
      } else if (clazz.equals(DictionaryEncodedStringValueIndex.class) || clazz.equals(DictionaryEncodedValueIndex.class)) {
        return (T) new GenericIndexedDictionaryEncodedStringValueIndex(bitmapFactory, dictionary, bitmaps);
      }
    }
    if (indexedTree != null && clazz.equals(SpatialIndex.class)) {
      return (T) (SpatialIndex) () -> indexedTree;
    }
    return null;
  }

  private abstract static class BaseGenericIndexedDictionaryEncodedIndex<T>
  {
    protected final BitmapFactory bitmapFactory;
    protected final Indexed<T> dictionary;
    protected final Indexed<ImmutableBitmap> bitmaps;

    protected BaseGenericIndexedDictionaryEncodedIndex(
        BitmapFactory bitmapFactory,
        GenericIndexed<T> dictionary,
        GenericIndexed<ImmutableBitmap> bitmaps
    )
    {
      this.bitmapFactory = bitmapFactory;
      this.dictionary = dictionary.singleThreaded();
      this.bitmaps = bitmaps.singleThreaded();
    }

    public ImmutableBitmap getBitmap(int idx)
    {
      if (idx < 0) {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }

      final ImmutableBitmap bitmap = bitmaps.get(idx);
      return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
    }
  }

  public static final class GenericIndexedDictionaryEncodedStringValueIndex
      extends BaseGenericIndexedDictionaryEncodedIndex<String> implements DictionaryEncodedStringValueIndex
  {
    public GenericIndexedDictionaryEncodedStringValueIndex(
        BitmapFactory bitmapFactory,
        GenericIndexed<String> dictionary,
        GenericIndexed<ImmutableBitmap> bitmaps
    )
    {
      super(bitmapFactory, dictionary, bitmaps);
    }

    @Override
    public int getCardinality()
    {
      return dictionary.size();
    }

    @Nullable
    @Override
    public String getValue(int index)
    {
      return dictionary.get(index);
    }

  }

  public static final class GenericIndexedDictionaryEncodedStringValueSetIndex
      extends BaseGenericIndexedDictionaryEncodedIndex<ByteBuffer> implements StringValueSetIndex, Utf8ValueSetIndex
  {
    private static final int SIZE_WORTH_CHECKING_MIN = 8;
    // This determines the cut-off point to swtich the merging algorithm from doing binary-search per element in the value
    // set to doing a sorted merge algorithm between value set and dictionary. The ratio here represents the ratio b/w
    // the number of elements in value set and the number of elements in the dictionary. The number has been derived
    // using benchmark in https://github.com/apache/druid/pull/13133. If the ratio is higher than the threshold, we use
    // sorted merge instead of binary-search based algorithm.
    private static final double SORTED_MERGE_RATIO_THRESHOLD = 0.12D;
    private final GenericIndexed<ByteBuffer> genericIndexedDictionary;

    public GenericIndexedDictionaryEncodedStringValueSetIndex(
        BitmapFactory bitmapFactory,
        GenericIndexed<ByteBuffer> dictionary,
        GenericIndexed<ImmutableBitmap> bitmaps
    )
    {
      super(bitmapFactory, dictionary, bitmaps);
      this.genericIndexedDictionary = dictionary;
    }

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return Math.min(1, (double) getBitmapForValue().size() / totalRows);
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {

          return bitmapResultFactory.wrapDimensionValue(getBitmapForValue());
        }

        private ImmutableBitmap getBitmapForValue()
        {
          final ByteBuffer valueUtf8 = value == null ? null : ByteBuffer.wrap(StringUtils.toUtf8(value));
          final int idx = dictionary.indexOf(valueUtf8);
          return getBitmap(idx);
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return getBitmapColumnIndexForSortedIterableUtf8(
          Iterables.transform(
              values,
              input -> ByteBuffer.wrap(StringUtils.toUtf8(input))
          ),
          values.size()
      );
    }

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

      return getBitmapColumnIndexForSortedIterableUtf8(tailSet, tailSet.size());
    }

    /**
     * Helper used by {@link #forSortedValues} and {@link #forSortedValuesUtf8}.
     */
    private BitmapColumnIndex getBitmapColumnIndexForSortedIterableUtf8(Iterable<ByteBuffer> valuesUtf8, int size)
    {
      // for large number of in-filter values in comparison to the dictionary size, use the sorted merge algorithm.
      if (size > SORTED_MERGE_RATIO_THRESHOLD * dictionary.size()) {
        return new SimpleImmutableBitmapIterableIndex()
        {
          @Override
          public Iterable<ImmutableBitmap> getBitmapIterable()
          {
            return () -> new Iterator<ImmutableBitmap>()
            {
              final PeekingIterator<ByteBuffer> valuesIterator = Iterators.peekingIterator(valuesUtf8.iterator());
              final PeekingIterator<ByteBuffer> dictionaryIterator =
                  Iterators.peekingIterator(genericIndexedDictionary.iterator());
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
                  ByteBuffer nextValue = valuesIterator.peek();
                  ByteBuffer nextDictionaryKey = dictionaryIterator.peek();
                  int comparison = GenericIndexed.BYTE_BUFFER_STRATEGY.compare(nextValue, nextDictionaryKey);
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

      // if the size of in-filter values is less than the threshold percentage of dictionary size, then use binary search
      // based lookup per value. The algorithm works well for smaller number of values.
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final PeekingIterator<ByteBuffer> valuesIterator = Iterators.peekingIterator(valuesUtf8.iterator());
            final int dictionarySize = dictionary.size();
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
              while (next < 0 && valuesIterator.hasNext()) {
                ByteBuffer nextValue = valuesIterator.next();
                next = dictionary.indexOf(nextValue);

                if (next == -dictionarySize - 1) {
                  // nextValue is past the end of the dictionary.
                  // Note: we can rely on indexOf returning (-(insertion point) - 1), even though Indexed doesn't
                  // guarantee it, because "dictionary" comes from GenericIndexed singleThreaded().
                  break;
                }
              }
            }
          };
        }
      };
    }
  }

  public static final class GenericIndexedDictionaryEncodedStringDruidPredicateIndex
      extends BaseGenericIndexedDictionaryEncodedIndex<String> implements DruidPredicateIndex
  {
    public GenericIndexedDictionaryEncodedStringDruidPredicateIndex(
        BitmapFactory bitmapFactory,
        GenericIndexed<String> dictionary,
        GenericIndexed<ImmutableBitmap> bitmaps
    )
    {
      super(bitmapFactory, dictionary, bitmaps);
    }

    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final Predicate<String> stringPredicate = matcherFactory.makeStringPredicate();
            final Iterator<String> iterator = dictionary.iterator();
            @Nullable
            String next = null;
            boolean nextSet = false;

            @Override
            public boolean hasNext()
            {
              if (!nextSet) {
                findNext();
              }
              return nextSet;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (!nextSet) {
                findNext();
                if (!nextSet) {
                  throw new NoSuchElementException();
                }
              }
              nextSet = false;
              final int idx = dictionary.indexOf(next);
              if (idx < 0) {
                return bitmapFactory.makeEmptyImmutableBitmap();
              }

              final ImmutableBitmap bitmap = bitmaps.get(idx);
              return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                String nextValue = iterator.next();
                nextSet = stringPredicate.apply(nextValue);
                if (nextSet) {
                  next = nextValue;
                }
              }
            }
          };
        }
      };
    }
  }

  public static final class GenericIndexedDictionaryEncodedColumnLexicographicalRangeIndex
      extends BaseGenericIndexedDictionaryEncodedIndex<ByteBuffer> implements LexicographicalRangeIndex
  {
    private final boolean hasNull;

    public GenericIndexedDictionaryEncodedColumnLexicographicalRangeIndex(
        BitmapFactory bitmapFactory,
        GenericIndexed<ByteBuffer> dictionary,
        GenericIndexed<ImmutableBitmap> bitmaps,
        boolean hasNull
    )
    {
      super(bitmapFactory, dictionary, bitmaps);
      this.hasNull = hasNull;
    }

    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      return new SimpleImmutableBitmapIterableIndex()
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
      };
    }

    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict,
        Predicate<String> matcher
    )
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
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
              while (currIndex < end && !applyMatcher(dictionary.get(currIndex))) {
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

        private boolean applyMatcher(@Nullable final ByteBuffer valueUtf8)
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
  }
}
