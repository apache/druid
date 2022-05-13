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
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;
import org.apache.druid.segment.column.SpatialIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.column.Utf8ValueSetIndex;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

public class DictionaryEncodedStringIndexSupplier implements ColumnIndexSupplier
{
  public static final ColumnIndexCapabilities CAPABILITIES = new SimpleColumnIndexCapabilities(true, true);

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
    if (bitmaps != null && clazz.equals(StringValueSetIndex.class)) {
      return (T) new GenericIndexedDictionaryEncodedStringValueSetIndex(bitmapFactory, dictionaryUtf8, bitmaps);
    } else if (bitmaps != null && clazz.equals(Utf8ValueSetIndex.class)) {
      return (T) new GenericIndexedDictionaryEncodedStringValueSetIndex(bitmapFactory, dictionaryUtf8, bitmaps);
    } else if (bitmaps != null && clazz.equals(DruidPredicateIndex.class)) {
      return (T) new GenericIndexedDictionaryEncodedStringDruidPredicateIndex(bitmapFactory, dictionary, bitmaps);
    } else if (bitmaps != null && clazz.equals(LexicographicalRangeIndex.class)) {
      return (T) new GenericIndexedDictionaryEncodedColumnLexicographicalRangeIndex(
          bitmapFactory,
          dictionaryUtf8,
          bitmaps
      );
    } else if (bitmaps != null && clazz.equals(DictionaryEncodedStringValueIndex.class)) {
      return (T) new GenericIndexedDictionaryEncodedStringValueIndex(bitmapFactory, dictionary, bitmaps);
    } else if (indexedTree != null && clazz.equals(SpatialIndex.class)) {
      return (T) (SpatialIndex) () -> indexedTree;
    }
    return null;
  }

  private abstract static class DictionaryEncodedStringBitmapColumnIndex implements BitmapColumnIndex
  {
    @Override
    public ColumnIndexCapabilities getIndexCapabilities()
    {
      return CAPABILITIES;
    }
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
    public boolean hasNulls()
    {
      return dictionary.indexOf(null) >= 0;
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

    @Override
    public int getIndex(@Nullable String value)
    {
      return dictionary.indexOf(value);
    }
  }

  public static final class GenericIndexedDictionaryEncodedStringValueSetIndex
      extends BaseGenericIndexedDictionaryEncodedIndex<ByteBuffer> implements StringValueSetIndex, Utf8ValueSetIndex
  {
    private static final int SIZE_WORTH_CHECKING_MIN = 8;

    public GenericIndexedDictionaryEncodedStringValueSetIndex(
        BitmapFactory bitmapFactory,
        GenericIndexed<ByteBuffer> dictionary,
        GenericIndexed<ImmutableBitmap> bitmaps
    )
    {
      super(bitmapFactory, dictionary, bitmaps);
    }

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new DictionaryEncodedStringBitmapColumnIndex()
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
          )
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

      return getBitmapColumnIndexForSortedIterableUtf8(tailSet);
    }

    /**
     * Helper used by {@link #forSortedValues} and {@link #forSortedValuesUtf8}.
     */
    private BitmapColumnIndex getBitmapColumnIndexForSortedIterableUtf8(Iterable<ByteBuffer> valuesUtf8)
    {
      return new DictionaryEncodedStringBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return Filters.estimateSelectivity(getBitmapsIterable().iterator(), totalRows);
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapsIterable());
        }

        private Iterable<ImmutableBitmap> getBitmapsIterable()
        {
          final int dictionarySize = dictionary.size();

          return () -> new Iterator<ImmutableBitmap>()
          {
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
      return new DictionaryEncodedStringBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return Filters.estimateSelectivity(
              getBitmapIterable().iterator(),
              totalRows
          );
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable());
        }

        private Iterable<ImmutableBitmap> getBitmapIterable()
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

    public GenericIndexedDictionaryEncodedColumnLexicographicalRangeIndex(
        BitmapFactory bitmapFactory,
        GenericIndexed<ByteBuffer> dictionary,
        GenericIndexed<ImmutableBitmap> bitmaps
    )
    {
      super(bitmapFactory, dictionary, bitmaps);
    }

    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      return new DictionaryEncodedStringBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return Filters.estimateSelectivity(getBitmapIterable().iterator(), totalRows);
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable());
        }

        private Iterable<ImmutableBitmap> getBitmapIterable()
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
        @Nullable Predicate<String> matcher
    )
    {
      return new DictionaryEncodedStringBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return Filters.estimateSelectivity(getBitmapIterable().iterator(), totalRows);
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {

          return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable());
        }

        private Iterable<ImmutableBitmap> getBitmapIterable()
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
          if (matcher == null) {
            return true;
          } else if (valueUtf8 == null) {
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
      int startIndex, endIndex;
      if (startValue == null) {
        startIndex = 0;
      } else {
        final String startValueToUse = NullHandling.emptyToNullIfNeeded(startValue);
        final int found = dictionary.indexOf(StringUtils.toUtf8ByteBuffer(startValueToUse));
        if (found >= 0) {
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
        if (found >= 0) {
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
