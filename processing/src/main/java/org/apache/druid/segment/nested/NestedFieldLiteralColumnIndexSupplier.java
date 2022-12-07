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

package org.apache.druid.segment.nested;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.primitives.Doubles;
import it.unimi.dsi.fastutil.doubles.DoubleArraySet;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.NumericRangeIndex;
import org.apache.druid.segment.column.SimpleBitmapColumnIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

/**
 * Supplies indexes for nested field columns {@link NestedFieldLiteralDictionaryEncodedColumn} of
 * {@link NestedDataComplexColumn}.
 */
public class NestedFieldLiteralColumnIndexSupplier<TStringDictionary extends Indexed<ByteBuffer>>
    implements ColumnIndexSupplier
{
  @Nullable
  private final ColumnType singleType;
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final Supplier<FixedIndexed<Integer>> localDictionarySupplier;
  private final Supplier<TStringDictionary> globalStringDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> globalLongDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> globalDoubleDictionarySupplier;

  private final int adjustLongId;
  private final int adjustDoubleId;

  public NestedFieldLiteralColumnIndexSupplier(
      NestedLiteralTypeInfo.TypeSet types,
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      Supplier<FixedIndexed<Integer>> localDictionarySupplier,
      Supplier<TStringDictionary> globalStringDictionarySupplier,
      Supplier<FixedIndexed<Long>> globalLongDictionarySupplier,
      Supplier<FixedIndexed<Double>> globalDoubleDictionarySupplier
  )
  {
    this.singleType = types.getSingleType();
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.localDictionarySupplier = localDictionarySupplier;
    this.globalStringDictionarySupplier = globalStringDictionarySupplier;
    this.globalLongDictionarySupplier = globalLongDictionarySupplier;
    this.globalDoubleDictionarySupplier = globalDoubleDictionarySupplier;
    this.adjustLongId = globalStringDictionarySupplier.get().size();
    this.adjustDoubleId = adjustLongId + globalLongDictionarySupplier.get().size();
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      final BitmapColumnIndex nullIndex;
      if (localDictionarySupplier.get().get(0) == 0) {
        // null index is always 0 in the global dictionary, even if there are no null rows in any of the literal columns
        nullIndex = new SimpleImmutableBitmapIndex(bitmaps.get(0));
      } else {
        nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
      }
      return (T) (NullValueIndex) () -> nullIndex;
    } else if (clazz.equals(DictionaryEncodedStringValueIndex.class) || clazz.equals(DictionaryEncodedValueIndex.class)) {
      return (T) new NestedLiteralDictionaryEncodedStringValueIndex();
    }

    if (singleType != null) {
      switch (singleType.getType()) {
        case STRING:
          if (clazz.equals(StringValueSetIndex.class)) {
            return (T) new NestedStringLiteralValueSetIndex();
          } else if (clazz.equals(LexicographicalRangeIndex.class)) {
            return (T) new NestedStringLiteralLexicographicalRangeIndex();
          } else if (clazz.equals(DruidPredicateIndex.class)) {
            return (T) new NestedStringLiteralPredicateIndex();
          }
          return null;
        case LONG:
          if (clazz.equals(StringValueSetIndex.class)) {
            return (T) new NestedLongLiteralValueSetIndex();
          } else if (clazz.equals(NumericRangeIndex.class)) {
            return (T) new NestedLongLiteralNumericRangeIndex();
          } else if (clazz.equals(DruidPredicateIndex.class)) {
            return (T) new NestedLongLiteralPredicateIndex();
          }
          return null;
        case DOUBLE:
          if (clazz.equals(StringValueSetIndex.class)) {
            return (T) new NestedDoubleLiteralValueSetIndex();
          } else if (clazz.equals(NumericRangeIndex.class)) {
            return (T) new NestedDoubleLiteralNumericRangeIndex();
          } else if (clazz.equals(DruidPredicateIndex.class)) {
            return (T) new NestedDoubleLiteralPredicateIndex();
          }
          return null;
        default:
          return null;
      }
    }
    if (clazz.equals(StringValueSetIndex.class)) {
      return (T) new NestedVariantLiteralValueSetIndex();
    } else if (clazz.equals(DruidPredicateIndex.class)) {
      return (T) new NestedVariantLiteralPredicateIndex();
    }
    return null;
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
   * Gets a value range from a global dictionary and maps it to a range on the local {@link #localDictionarySupplier}.
   * The starting index of the resulting range is inclusive, while the endpoint is exclusive [start, end)
   */
  private <T> IntIntPair getLocalRangeFromDictionary(
      @Nullable T startValue,
      boolean startStrict,
      @Nullable T endValue,
      boolean endStrict,
      Indexed<Integer> localDictionary,
      Indexed<T> globalDictionary,
      int adjust
  )
  {
    int globalStartIndex, globalEndIndex;
    int localStartIndex, localEndIndex;
    // start with standard range finding in global value dictionary
    if (startValue == null) {
      globalStartIndex = adjust == 0 ? 1 : adjust; // global index 0 is always the null value
    } else {
      final int found = globalDictionary.indexOf(startValue);
      if (found >= 0) {
        globalStartIndex = adjust + (startStrict ? found + 1 : found);
      } else {
        globalStartIndex = adjust + (-(found + 1));
      }
    }

    if (endValue == null) {
      globalEndIndex = globalDictionary.size() + adjust;
    } else {
      final int found = globalDictionary.indexOf(endValue);
      if (found >= 0) {
        globalEndIndex = adjust + (endStrict ? found : found + 1);
      } else {
        globalEndIndex = adjust + (-(found + 1));
      }
    }
    globalEndIndex = Math.max(globalStartIndex, globalEndIndex);

    if (globalStartIndex == globalEndIndex) {
      return new IntIntImmutablePair(0, 0);
    }

    // with global dictionary id range settled, now lets map that onto a local dictionary id range
    int localFound = localDictionary.indexOf(globalStartIndex);
    if (localFound < 0) {
      // the first valid global index is not within the local dictionary, so the insertion point is where we begin
      localStartIndex = -(localFound + 1);
    } else {
      // valid global index in local dictionary, start here
      localStartIndex = localFound;
    }

    int localEndFound = localDictionary.indexOf(globalEndIndex);
    if (localEndFound < 0) {
      localEndIndex = -(localEndFound + 1);
    } else {
      localEndIndex = localEndFound;
    }

    localStartIndex = Math.min(localStartIndex, localDictionary.size());
    localEndIndex = Math.max(localStartIndex, Math.min(localDictionary.size(), localEndIndex));


    return new IntIntImmutablePair(localStartIndex, localEndIndex);
  }


  private <T> BitmapColumnIndex makeRangeIndex(
      @Nullable T startValue,
      boolean startStrict,
      @Nullable T endValue,
      boolean endStrict,
      Indexed<Integer> localDictionary,
      Indexed<T> globalDictionary,
      int adjust
  )
  {
    final IntIntPair localRange = getLocalRangeFromDictionary(
        startValue,
        startStrict,
        endValue,
        endStrict,
        localDictionary,
        globalDictionary,
        adjust
    );
    final int startIndex = localRange.leftInt();
    final int endIndex = localRange.rightInt();
    return new SimpleImmutableBitmapIterableIndex()
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          final IntIterator rangeIterator = IntListUtils.fromTo(startIndex, endIndex).iterator();

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

  private class NestedLiteralDictionaryEncodedStringValueIndex implements DictionaryEncodedStringValueIndex
  {
    final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
    final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
    final FixedIndexed<Long> longDictionary = globalLongDictionarySupplier.get();
    final FixedIndexed<Double> doubleDictionary = globalDoubleDictionarySupplier.get();

    @Override
    public int getCardinality()
    {
      return localDictionary.size();
    }

    @Nullable
    @Override
    public String getValue(int index)
    {
      int globalIndex = localDictionary.get(index);
      if (globalIndex < adjustLongId) {
        return StringUtils.fromUtf8Nullable(stringDictionary.get(globalIndex));
      } else if (globalIndex < adjustDoubleId) {
        return String.valueOf(longDictionary.get(globalIndex - adjustLongId));
      } else {
        return String.valueOf(doubleDictionary.get(globalIndex - adjustDoubleId));
      }
    }

    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      return NestedFieldLiteralColumnIndexSupplier.this.getBitmap(idx);
    }
  }

  private class NestedStringLiteralValueSetIndex implements StringValueSetIndex
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleBitmapColumnIndex()
      {
        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return (double) getBitmap(
              localDictionary.indexOf(stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(value)))
          ).size() / totalRows;
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.wrapDimensionValue(
              getBitmap(localDictionary.indexOf(stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(value))))
          );
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
            final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
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
                next = localDictionary.indexOf(stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(nextValue)));
              }
            }
          };
        }
      };
    }
  }

  private class NestedStringLiteralLexicographicalRangeIndex implements LexicographicalRangeIndex
  {
    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      return makeRangeIndex(
          StringUtils.toUtf8ByteBuffer(NullHandling.emptyToNullIfNeeded(startValue)),
          startStrict,
          StringUtils.toUtf8ByteBuffer(NullHandling.emptyToNullIfNeeded(endValue)),
          endStrict,
          localDictionarySupplier.get(),
          globalStringDictionarySupplier.get(),
          0
      );
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

          final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
          final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
          final IntIntPair range = getLocalRangeFromDictionary(
              StringUtils.toUtf8ByteBuffer(startValue),
              startStrict,
              StringUtils.toUtf8ByteBuffer(endValue),
              endStrict,
              localDictionary,
              stringDictionary,
              0
          );
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
              while (currIndex < end && !matcher.apply(StringUtils.fromUtf8Nullable(stringDictionary.get(localDictionary.get(currIndex))))) {
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
      };
    }
  }

  private class NestedStringLiteralPredicateIndex implements DruidPredicateIndex
  {
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

            final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
            final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
            final Predicate<String> stringPredicate = matcherFactory.makeStringPredicate();

            // in the future, this could use an int iterator
            final Iterator<Integer> iterator = localDictionary.iterator();
            int next;
            int index = 0;
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
              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                nextSet = stringPredicate.apply(StringUtils.fromUtf8Nullable(stringDictionary.get(nextValue)));
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }

  private class NestedLongLiteralValueSetIndex implements StringValueSetIndex
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      final Long longValue = GuavaUtils.tryParseLong(value);
      return new SimpleBitmapColumnIndex()
      {

        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final FixedIndexed<Long> longDictionary = globalLongDictionarySupplier.get();
        @Override
        public double estimateSelectivity(int totalRows)
        {
          if (longValue == null) {
            return (double) getBitmap(localDictionary.indexOf(0)).size() / totalRows;
          }
          return (double) getBitmap(
              localDictionary.indexOf(longDictionary.indexOf(longValue) + adjustLongId)
          ).size() / totalRows;
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          if (longValue == null) {
            return bitmapResultFactory.wrapDimensionValue(getBitmap(localDictionary.indexOf(0)));
          }
          return bitmapResultFactory.wrapDimensionValue(
              getBitmap(localDictionary.indexOf(longDictionary.indexOf(longValue) + adjustLongId))
          );
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          LongSet longs = new LongArraySet(values.size());
          boolean needNullCheck = false;
          for (String value : values) {
            if (value == null) {
              needNullCheck = true;
            } else {
              Long theValue = GuavaUtils.tryParseLong(value);
              if (theValue != null) {
                longs.add(theValue.longValue());
              }
            }
          }
          final boolean doNullCheck = needNullCheck;
          return () -> new Iterator<ImmutableBitmap>()
          {
            final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
            final FixedIndexed<Long> longDictionary = globalLongDictionarySupplier.get();
            final LongIterator iterator = longs.iterator();
            int next = -1;
            boolean nullChecked = false;

            @Override
            public boolean hasNext()
            {
              if (doNullCheck && !nullChecked) {
                return true;
              }
              if (next < 0) {
                findNext();
              }
              return next >= 0;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (doNullCheck && !nullChecked) {
                nullChecked = true;
                return getBitmap(0);
              }
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
                long nextValue = iterator.nextLong();
                next = localDictionary.indexOf(longDictionary.indexOf(nextValue) + adjustLongId);
              }
            }
          };
        }
      };
    }
  }

  private class NestedLongLiteralNumericRangeIndex implements NumericRangeIndex
  {
    @Override
    public BitmapColumnIndex forRange(
        @Nullable Number startValue,
        boolean startStrict,
        @Nullable Number endValue,
        boolean endStrict
    )
    {
      return makeRangeIndex(
          startValue != null ? startValue.longValue() : null,
          startStrict,
          endValue != null ? endValue.longValue() : null,
          endStrict,
          localDictionarySupplier.get(),
          globalLongDictionarySupplier.get(),
          adjustLongId
      );
    }
  }

  private class NestedLongLiteralPredicateIndex implements DruidPredicateIndex
  {
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
            final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
            final FixedIndexed<Long> longDictionary = globalLongDictionarySupplier.get();
            final DruidLongPredicate longPredicate = matcherFactory.makeLongPredicate();

            // in the future, this could use an int iterator
            final Iterator<Integer> iterator = localDictionary.iterator();
            int next;
            int index = 0;
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

              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                if (nextValue == 0) {
                  nextSet = longPredicate.applyNull();
                } else {
                  nextSet = longPredicate.applyLong(longDictionary.get(nextValue - adjustLongId));
                }
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }

  private class NestedDoubleLiteralValueSetIndex implements StringValueSetIndex
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      final Double doubleValue = Strings.isNullOrEmpty(value) ? null : Doubles.tryParse(value);
      return new SimpleBitmapColumnIndex()
      {
        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final FixedIndexed<Double> doubleDictionary = globalDoubleDictionarySupplier.get();
        @Override
        public double estimateSelectivity(int totalRows)
        {
          if (doubleValue == null) {
            return (double) getBitmap(localDictionary.indexOf(0)).size() / totalRows;
          }
          return (double) getBitmap(
              localDictionary.indexOf(doubleDictionary.indexOf(doubleValue) + adjustDoubleId)
          ).size() / totalRows;
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          if (doubleValue == null) {
            return bitmapResultFactory.wrapDimensionValue(getBitmap(localDictionary.indexOf(0)));
          }
          return bitmapResultFactory.wrapDimensionValue(
              getBitmap(localDictionary.indexOf(doubleDictionary.indexOf(doubleValue) + adjustDoubleId))
          );
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          DoubleSet doubles = new DoubleArraySet(values.size());
          boolean needNullCheck = false;
          for (String value : values) {
            if (value == null) {
              needNullCheck = true;
            } else {
              Double theValue = Doubles.tryParse(value);
              if (theValue != null) {
                doubles.add(theValue.doubleValue());
              }
            }
          }
          final boolean doNullCheck = needNullCheck;
          return () -> new Iterator<ImmutableBitmap>()
          {
            final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
            final FixedIndexed<Double> doubleDictionary = globalDoubleDictionarySupplier.get();
            final DoubleIterator iterator = doubles.iterator();
            int next = -1;
            boolean nullChecked = false;

            @Override
            public boolean hasNext()
            {
              if (doNullCheck && !nullChecked) {
                return true;
              }
              if (next < 0) {
                findNext();
              }
              return next >= 0;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (doNullCheck && !nullChecked) {
                nullChecked = true;
                return getBitmap(0);
              }
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
                double nextValue = iterator.nextDouble();
                next = localDictionary.indexOf(doubleDictionary.indexOf(nextValue) + adjustDoubleId);
              }
            }
          };
        }
      };
    }
  }

  private class NestedDoubleLiteralNumericRangeIndex implements NumericRangeIndex
  {
    @Override
    public BitmapColumnIndex forRange(
        @Nullable Number startValue,
        boolean startStrict,
        @Nullable Number endValue,
        boolean endStrict
    )
    {
      return makeRangeIndex(
          startValue != null ? startValue.doubleValue() : null,
          startStrict,
          endValue != null ? endValue.doubleValue() : null,
          endStrict,
          localDictionarySupplier.get(),
          globalDoubleDictionarySupplier.get(),
          adjustDoubleId
      );
    }
  }

  private class NestedDoubleLiteralPredicateIndex implements DruidPredicateIndex
  {
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
            final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
            final FixedIndexed<Double> doubleDictionary = globalDoubleDictionarySupplier.get();
            final DruidDoublePredicate doublePredicate = matcherFactory.makeDoublePredicate();

            // in the future, this could use an int iterator
            final Iterator<Integer> iterator = localDictionary.iterator();
            int next;
            int index = 0;
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
              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                if (nextValue == 0) {
                  nextSet = doublePredicate.applyNull();
                } else {
                  nextSet = doublePredicate.applyDouble(doubleDictionary.get(nextValue - adjustDoubleId));
                }
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }

  private abstract class NestedVariantLiteralIndex
  {
    final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
    final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
    final FixedIndexed<Long> longDictionary = globalLongDictionarySupplier.get();
    final FixedIndexed<Double> doubleDictionary = globalDoubleDictionarySupplier.get();

    IntList getIndexes(@Nullable String value)
    {
      IntList intList = new IntArrayList();
      if (value == null) {
        intList.add(localDictionary.indexOf(0));
        return intList;
      }

      // multi-type, return all that match
      int globalId = stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(value));
      int localId = localDictionary.indexOf(globalId);
      if (localId >= 0) {
        intList.add(localId);
      }
      Long someLong = GuavaUtils.tryParseLong(value);
      if (someLong != null) {
        globalId = longDictionary.indexOf(someLong);
        localId = localDictionary.indexOf(globalId + adjustLongId);
        if (localId >= 0) {
          intList.add(localId);
        }
      }

      Double someDouble = Doubles.tryParse(value);
      if (someDouble != null) {
        globalId = doubleDictionary.indexOf(someDouble);
        localId = localDictionary.indexOf(globalId + adjustDoubleId);
        if (localId >= 0) {
          intList.add(localId);
        }
      }
      return intList;
    }
  }

  /**
   * {@link StringValueSetIndex} but for variant typed nested literal columns
   */
  private class NestedVariantLiteralValueSetIndex extends NestedVariantLiteralIndex implements StringValueSetIndex
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        protected Iterable<ImmutableBitmap> getBitmapIterable()
        {
          final IntIterator iterator = getIndexes(value).iterator();
          return () -> new Iterator<ImmutableBitmap>()
          {
            @Override
            public boolean hasNext()
            {
              return iterator.hasNext();
            }

            @Override
            public ImmutableBitmap next()
            {
              return getBitmap(iterator.nextInt());
            }
          };
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final Iterator<String> iterator = values.iterator();
            IntIterator nextIterator = null;

            @Override
            public boolean hasNext()
            {
              if (nextIterator == null || !nextIterator.hasNext()) {
                findNext();
              }
              return nextIterator.hasNext();
            }

            @Override
            public ImmutableBitmap next()
            {
              if (nextIterator == null || !nextIterator.hasNext()) {
                findNext();
                if (!nextIterator.hasNext()) {
                  throw new NoSuchElementException();
                }
              }
              return getBitmap(nextIterator.nextInt());
            }

            private void findNext()
            {
              while ((nextIterator == null || !nextIterator.hasNext()) && iterator.hasNext()) {
                String nextValue = iterator.next();
                nextIterator = getIndexes(nextValue).iterator();
              }
            }
          };
        }
      };
    }
  }

  /**
   * {@link DruidPredicateIndex} but for variant typed nested literal columns
   */
  private class NestedVariantLiteralPredicateIndex extends NestedVariantLiteralIndex implements DruidPredicateIndex
  {
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
            final DruidLongPredicate longPredicate = matcherFactory.makeLongPredicate();
            final DruidDoublePredicate doublePredicate = matcherFactory.makeDoublePredicate();

            // in the future, this could use an int iterator
            final Iterator<Integer> iterator = localDictionary.iterator();
            int next;
            int index;
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
              return getBitmap(next);
            }

            private void findNext()
            {
              while (!nextSet && iterator.hasNext()) {
                Integer nextValue = iterator.next();
                if (nextValue >= adjustDoubleId) {
                  nextSet = doublePredicate.applyDouble(doubleDictionary.get(nextValue - adjustDoubleId));
                } else if (nextValue >= adjustLongId) {
                  nextSet = longPredicate.applyLong(longDictionary.get(nextValue - adjustLongId));
                } else {
                  nextSet = stringPredicate.apply(StringUtils.fromUtf8Nullable(stringDictionary.get(nextValue)));
                }
                if (nextSet) {
                  next = index;
                }
                index++;
              }
            }
          };
        }
      };
    }
  }
}
