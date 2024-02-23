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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
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
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.index.AllFalseBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleBitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapDelegatingIterableIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.index.semantic.ArrayElementIndexes;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.NumericRangeIndexes;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

/**
 * Supplies indexes for nested field columns {@link NestedFieldDictionaryEncodedColumn} of
 * {@link NestedDataComplexColumn}.
 */
public class NestedFieldColumnIndexSupplier<TStringDictionary extends Indexed<ByteBuffer>>
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

  private final Supplier<FrontCodedIntArrayIndexed> globalArrayDictionarySupplier;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  @Nullable
  private final GenericIndexed<ImmutableBitmap> arrayElementBitmaps;
  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  @Nullable
  private final Supplier<FixedIndexed<Integer>> arrayElementDictionarySupplier;

  private final int adjustLongId;
  private final int adjustDoubleId;
  private final int adjustArrayId;
  private final ColumnConfig columnConfig;
  private final int numRows;

  public NestedFieldColumnIndexSupplier(
      FieldTypeInfo.TypeSet types,
      BitmapFactory bitmapFactory,
      ColumnConfig columnConfig,
      GenericIndexed<ImmutableBitmap> bitmaps,
      Supplier<FixedIndexed<Integer>> localDictionarySupplier,
      Supplier<TStringDictionary> globalStringDictionarySupplier,
      Supplier<FixedIndexed<Long>> globalLongDictionarySupplier,
      Supplier<FixedIndexed<Double>> globalDoubleDictionarySupplier,
      @Nullable Supplier<FrontCodedIntArrayIndexed> globalArrayDictionarySupplier,
      @Nullable Supplier<FixedIndexed<Integer>> arrayElementDictionarySupplier,
      @Nullable GenericIndexed<ImmutableBitmap> arrayElementBitmaps,
      int numRows
  )
  {
    this.singleType = types.getSingleType();
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.localDictionarySupplier = localDictionarySupplier;
    this.globalStringDictionarySupplier = globalStringDictionarySupplier;
    this.globalLongDictionarySupplier = globalLongDictionarySupplier;
    this.globalDoubleDictionarySupplier = globalDoubleDictionarySupplier;
    this.globalArrayDictionarySupplier = globalArrayDictionarySupplier;
    this.arrayElementDictionarySupplier = arrayElementDictionarySupplier;
    this.arrayElementBitmaps = arrayElementBitmaps;
    this.adjustLongId = globalStringDictionarySupplier.get().size();
    this.adjustDoubleId = adjustLongId + globalLongDictionarySupplier.get().size();
    this.adjustArrayId = adjustDoubleId + globalDoubleDictionarySupplier.get().size();
    this.columnConfig = columnConfig;
    this.numRows = numRows;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      final BitmapColumnIndex nullIndex;
      if (localDictionarySupplier.get().get(0) == 0) {
        // null index is always 0 in the global dictionary, even if there are no null rows in any of the nested fields
        nullIndex = new SimpleImmutableBitmapIndex(bitmaps.get(0));
      } else {
        nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
      }
      return (T) (NullValueIndex) () -> nullIndex;
    } else if (clazz.equals(DictionaryEncodedStringValueIndex.class) || clazz.equals(DictionaryEncodedValueIndex.class)) {
      return (T) new NestedFieldDictionaryEncodedStringValueIndex();
    }

    if (singleType != null) {
      switch (singleType.getType()) {
        case STRING:
          if (clazz.equals(StringValueSetIndexes.class)) {
            return (T) new NestedStringValueSetIndexes();
          } else if (clazz.equals(LexicographicalRangeIndexes.class)) {
            return (T) new NestedStringLexicographicalRangeIndexes();
          } else if (clazz.equals(DruidPredicateIndexes.class)) {
            return (T) new NestedStringPredicateIndexes();
          }
          return null;
        case LONG:
          if (clazz.equals(ValueIndexes.class)) {
            return (T) new NestedLongValueIndexes();
          } else if (clazz.equals(StringValueSetIndexes.class)) {
            return (T) new NestedLongStringValueSetIndex();
          } else if (clazz.equals(NumericRangeIndexes.class)) {
            return (T) new NestedLongNumericRangeIndexes();
          } else if (clazz.equals(DruidPredicateIndexes.class)) {
            return (T) new NestedLongPredicateIndexes();
          }
          return null;
        case DOUBLE:
          if (clazz.equals(ValueIndexes.class)) {
            return (T) new NestedDoubleValueIndexes();
          } else if (clazz.equals(StringValueSetIndexes.class)) {
            return (T) new NestedDoubleStringValueSetIndex();
          } else if (clazz.equals(NumericRangeIndexes.class)) {
            return (T) new NestedDoubleNumericRangeIndexes();
          } else if (clazz.equals(DruidPredicateIndexes.class)) {
            return (T) new NestedDoublePredicateIndexes();
          }
          return null;
        case ARRAY:
          if (clazz.equals(ValueIndexes.class)) {
            return (T) new NestedArrayValueIndexes();
          } else if (clazz.equals(ArrayElementIndexes.class)) {
            return (T) new NestedArrayElementIndexes();
          }
          return null;
        default:
          return null;
      }
    }
    if (clazz.equals(StringValueSetIndexes.class)) {
      return (T) new NestedVariantStringValueSetIndexes();
    } else if (clazz.equals(DruidPredicateIndexes.class)) {
      return (T) new NestedVariantPredicateIndexes();
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


  @Nullable
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
    final int size = endIndex - startIndex;
    if (ColumnIndexSupplier.skipComputingRangeIndexes(columnConfig, numRows, size)) {
      return null;
    }
    return new SimpleImmutableBitmapDelegatingIterableIndex()
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

      @Nullable
      @Override
      protected ImmutableBitmap getUnknownsBitmap()
      {
        if (localDictionary.get(0) == 0) {
          return bitmaps.get(0);
        }
        return null;
      }
    };
  }

  private class NestedFieldDictionaryEncodedStringValueIndex implements DictionaryEncodedStringValueIndex
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
    public BitmapFactory getBitmapFactory()
    {
      return bitmapFactory;
    }

    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      return NestedFieldColumnIndexSupplier.this.getBitmap(idx);
    }
  }

  private class NestedStringValueSetIndexes implements StringValueSetIndexes
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleBitmapColumnIndex()
      {
        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int globalId = stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(value));
          if (includeUnknown && localDictionary.get(0) == 0) {
            if (globalId < 0) {
              return bitmapResultFactory.wrapDimensionValue(bitmaps.get(0));
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(
                    getBitmap(localDictionary.indexOf(globalId)),
                    bitmaps.get(0)
                )
            );
          }
          if (globalId < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(localDictionary.indexOf(globalId)));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapDelegatingIterableIndex()
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

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          if (!values.contains(null) && localDictionarySupplier.get().get(0) == 0) {
            return bitmaps.get(0);
          }
          return null;
        }
      };
    }
  }

  private class NestedStringLexicographicalRangeIndexes implements LexicographicalRangeIndexes
  {
    @Override
    @Nullable
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
    @Nullable
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict,
        DruidObjectPredicate<String> matcher
    )
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
              while (currIndex < end && !matcher.apply(StringUtils.fromUtf8Nullable(stringDictionary.get(localDictionary.get(currIndex)))).matches(false)) {
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
          if (localDictionary.get(0) == 0) {
            return bitmaps.get(0);
          }
          return null;
        }
      };
    }
  }

  private class NestedStringPredicateIndexes implements DruidPredicateIndexes
  {
    @Override
    @Nullable
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
      if (ColumnIndexSupplier.skipComputingPredicateIndexes(columnConfig, numRows, localDictionary.size())) {
        return null;
      }
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable(boolean includeUnknown)
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
            final DruidObjectPredicate<String> stringPredicate = matcherFactory.makeStringPredicate();

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
                nextSet = stringPredicate.apply(StringUtils.fromUtf8Nullable(stringDictionary.get(nextValue)))
                                         .matches(includeUnknown);
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

  private class NestedLongValueIndexes implements ValueIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType)
    {
      final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(valueType), value);
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, ExpressionType.LONG);
      final ImmutableBitmap nullValueBitmap = localDictionarySupplier.get().get(0) == 0
                                              ? bitmaps.get(0)
                                              : bitmapFactory.makeEmptyImmutableBitmap();
      if (castForComparison == null) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final long longValue = castForComparison.asLong();


      return new SimpleBitmapColumnIndex()
      {
        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final FixedIndexed<Long> globalDictionary = globalLongDictionarySupplier.get();

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int globalId = globalDictionary.indexOf(longValue);
          if (globalId < 0) {
            if (includeUnknown) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          final int id = localDictionary.indexOf(globalId + adjustLongId);
          if (includeUnknown) {
            if (id < 0) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(getBitmap(id), nullValueBitmap)
            );
          }
          if (id < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(id));
        }
      };
    }
  }

  private class NestedLongStringValueSetIndex implements StringValueSetIndexes
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      final boolean inputNull = value == null;
      final Long longValue = GuavaUtils.tryParseLong(value);
      return new SimpleBitmapColumnIndex()
      {

        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final FixedIndexed<Long> longDictionary = globalLongDictionarySupplier.get();

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          if (longValue == null) {
            if (inputNull) {
              return bitmapResultFactory.wrapDimensionValue(getBitmap(localDictionary.indexOf(0)));
            } else {
              return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
            }
          }
          final int globalId = longDictionary.indexOf(longValue);
          if (includeUnknown && localDictionary.get(0) == 0) {
            if (globalId < 0) {
              return bitmapResultFactory.wrapDimensionValue(bitmaps.get(0));
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(
                    getBitmap(localDictionary.indexOf(globalId + adjustLongId)),
                    bitmaps.get(0)
                )
            );
          }
          if (globalId < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(localDictionary.indexOf(globalId + adjustLongId)));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapDelegatingIterableIndex()
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

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          if (!values.contains(null) && localDictionarySupplier.get().get(0) == 0) {
            return bitmaps.get(0);
          }
          return null;
        }
      };
    }
  }

  private class NestedLongNumericRangeIndexes implements NumericRangeIndexes
  {
    @Override
    @Nullable
    public BitmapColumnIndex forRange(
        @Nullable Number startValue,
        boolean startStrict,
        @Nullable Number endValue,
        boolean endStrict
    )
    {
      final Long startLong;
      final Long endLong;
      if (startValue == null) {
        startLong = null;
      } else if (startStrict) {
        startLong = (long) Math.floor(startValue.doubleValue());
      } else {
        startLong = (long) Math.ceil(startValue.doubleValue());
      }
      if (endValue == null) {
        endLong = null;
      } else if (endStrict) {
        endLong = (long) Math.ceil(endValue.doubleValue());
      } else {
        endLong = (long) Math.floor(endValue.doubleValue());
      }
      return makeRangeIndex(
          startLong,
          startStrict,
          endLong,
          endStrict,
          localDictionarySupplier.get(),
          globalLongDictionarySupplier.get(),
          adjustLongId
      );
    }
  }

  private class NestedLongPredicateIndexes implements DruidPredicateIndexes
  {
    @Override
    @Nullable
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
      if (ColumnIndexSupplier.skipComputingPredicateIndexes(columnConfig, numRows, localDictionary.size())) {
        return null;
      }
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable(boolean includeUnknown)
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
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
                  nextSet = longPredicate.applyNull().matches(includeUnknown);
                } else {
                  nextSet = longPredicate.applyLong(longDictionary.get(nextValue - adjustLongId)).matches(includeUnknown);
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

  private class NestedDoubleValueIndexes implements ValueIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType)
    {
      final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(valueType), value);
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, ExpressionType.DOUBLE);
      final ImmutableBitmap nullValueBitmap = localDictionarySupplier.get().get(0) == 0
                                              ? bitmaps.get(0)
                                              : bitmapFactory.makeEmptyImmutableBitmap();
      if (castForComparison == null) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final double doubleValue = castForComparison.asDouble();

      return new SimpleBitmapColumnIndex()
      {
        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final FixedIndexed<Double> globalDictionary = globalDoubleDictionarySupplier.get();

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int globalId = globalDictionary.indexOf(doubleValue);
          if (globalId < 0) {
            if (includeUnknown) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          final int id = localDictionary.indexOf(globalId + adjustDoubleId);
          if (includeUnknown) {
            if (id < 0) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(getBitmap(id), nullValueBitmap)
            );
          }
          if (id < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(id));
        }
      };
    }
  }

  private class NestedDoubleStringValueSetIndex implements StringValueSetIndexes
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      final boolean inputNull = value == null;
      final Double doubleValue = Strings.isNullOrEmpty(value) ? null : Doubles.tryParse(value);
      return new SimpleBitmapColumnIndex()
      {
        final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
        final FixedIndexed<Double> doubleDictionary = globalDoubleDictionarySupplier.get();

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          if (doubleValue == null) {
            if (inputNull) {
              return bitmapResultFactory.wrapDimensionValue(getBitmap(localDictionary.indexOf(0)));
            } else {
              return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
            }
          }
          final int globalId = doubleDictionary.indexOf(doubleValue);
          if (includeUnknown && localDictionary.get(0) == 0) {
            if (globalId < 0) {
              return bitmapResultFactory.wrapDimensionValue(bitmaps.get(0));
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(
                    getBitmap(localDictionary.indexOf(globalId + adjustDoubleId)),
                    bitmaps.get(0)
                )
            );
          }
          if (globalId < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(localDictionary.indexOf(globalId + adjustDoubleId)));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapDelegatingIterableIndex()
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

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          if (!values.contains(null) && localDictionarySupplier.get().get(0) == 0) {
            return bitmaps.get(0);
          }
          return null;
        }
      };
    }
  }

  private class NestedDoubleNumericRangeIndexes implements NumericRangeIndexes
  {
    @Override
    @Nullable
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

  private class NestedDoublePredicateIndexes implements DruidPredicateIndexes
  {
    @Override
    @Nullable
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
      if (ColumnIndexSupplier.skipComputingPredicateIndexes(columnConfig, numRows, localDictionary.size())) {
        return null;
      }
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable(boolean includeUnknown)
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
                  nextSet = doublePredicate.applyNull().matches(includeUnknown);
                } else {
                  nextSet = doublePredicate.applyDouble(doubleDictionary.get(nextValue - adjustDoubleId))
                                           .matches(includeUnknown);
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

  private abstract class NestedVariantIndexes
  {
    final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
    final Indexed<ByteBuffer> stringDictionary = globalStringDictionarySupplier.get();
    final FixedIndexed<Long> longDictionary = globalLongDictionarySupplier.get();
    final FixedIndexed<Double> doubleDictionary = globalDoubleDictionarySupplier.get();
    @Nullable
    final FrontCodedIntArrayIndexed arrayDictionary = globalArrayDictionarySupplier == null
                                                      ? null
                                                      : globalArrayDictionarySupplier.get();

    IntList getIndexes(@Nullable String value)
    {
      IntList intList = new IntArrayList();
      if (value == null) {
        intList.add(localDictionary.indexOf(0));
        return intList;
      }

      // multi-type, return all that match
      int globalId = stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(value));
      if (globalId >= 0) {
        int localId = localDictionary.indexOf(globalId);
        if (localId >= 0) {
          intList.add(localId);
        }
      }
      Long someLong = GuavaUtils.tryParseLong(value);
      if (someLong != null) {
        globalId = longDictionary.indexOf(someLong);
        if (globalId >= 0) {
          int localId = localDictionary.indexOf(globalId + adjustLongId);
          if (localId >= 0) {
            intList.add(localId);
          }
        }
      }

      Double someDouble = Doubles.tryParse(value);
      if (someDouble != null) {
        globalId = doubleDictionary.indexOf(someDouble);
        if (globalId >= 0) {
          int localId = localDictionary.indexOf(globalId + adjustDoubleId);
          if (localId >= 0) {
            intList.add(localId);
          }
        }
      }
      return intList;
    }
  }

  /**
   * {@link StringValueSetIndexes} but for variant typed nested columns
   */
  private class NestedVariantStringValueSetIndexes extends NestedVariantIndexes implements StringValueSetIndexes
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleImmutableBitmapDelegatingIterableIndex()
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

        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          if (value != null && localDictionarySupplier.get().get(0) == 0) {
            return bitmaps.get(0);
          }
          return null;
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapDelegatingIterableIndex()
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

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          if (!values.contains(null) && localDictionarySupplier.get().get(0) == 0) {
            return bitmaps.get(0);
          }
          return null;
        }
      };
    }
  }

  /**
   * {@link DruidPredicateIndexes} but for variant typed nested fields
   */
  private class NestedVariantPredicateIndexes extends NestedVariantIndexes implements DruidPredicateIndexes
  {
    @Override
    @Nullable
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      if (ColumnIndexSupplier.skipComputingPredicateIndexes(columnConfig, numRows, localDictionary.size())) {
        return null;
      }
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable(boolean includeUnknown)
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final DruidObjectPredicate<String> stringPredicate = matcherFactory.makeStringPredicate();
            final DruidLongPredicate longPredicate = matcherFactory.makeLongPredicate();
            final DruidDoublePredicate doublePredicate = matcherFactory.makeDoublePredicate();

            final Supplier<DruidObjectPredicate<Object[]>> arrayPredicateSupplier = Suppliers.memoize(
                () -> matcherFactory.makeArrayPredicate(singleType)
            );

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
                if (nextValue >= adjustArrayId) {
                  // this shouldn't be possible since arrayIds will only exist if array dictionary is not null
                  // v4 columns however have a null array dictionary
                  Preconditions.checkNotNull(arrayDictionary);
                  final int[] array = arrayDictionary.get(nextValue - adjustArrayId);
                  final Object[] arrayObj = new Object[array.length];
                  for (int i = 0; i < arrayObj.length; i++) {
                    if (array[i] == 0) {
                      arrayObj[i] = null;
                    } else if (array[i] >= adjustDoubleId) {
                      arrayObj[i] = doubleDictionary.get(array[i] - adjustDoubleId);
                    } else if (array[i] >= adjustLongId) {
                      arrayObj[i] = longDictionary.get(array[i] - adjustLongId);
                    } else {
                      arrayObj[i] = StringUtils.fromUtf8Nullable(stringDictionary.get(array[i]));
                    }
                  }
                  nextSet = arrayPredicateSupplier.get().apply(arrayObj).matches(includeUnknown);
                } else if (nextValue >= adjustDoubleId) {
                  nextSet = doublePredicate.applyDouble(doubleDictionary.get(nextValue - adjustDoubleId))
                                           .matches(includeUnknown);
                } else if (nextValue >= adjustLongId) {
                  nextSet = longPredicate.applyLong(longDictionary.get(nextValue - adjustLongId))
                                         .matches(includeUnknown);
                } else {
                  nextSet = stringPredicate.apply(StringUtils.fromUtf8Nullable(stringDictionary.get(nextValue)))
                                           .matches(includeUnknown);
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

  private class NestedArrayValueIndexes implements ValueIndexes
  {
    private final ImmutableBitmap nullValueBitmap = localDictionarySupplier.get().get(0) == 0
                                                    ? bitmaps.get(0)
                                                    : bitmapFactory.makeEmptyImmutableBitmap();

    @Nullable
    @Override
    public BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType)
    {
      if (!valueType.isArray()) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(valueType), value);
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(
          eval,
          ExpressionType.fromColumnTypeStrict(singleType)
      );
      if (castForComparison == null) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final Object[] arrayToMatch = castForComparison.asArray();
      Indexed elements;
      final int elementOffset;

      switch (singleType.getElementType().getType()) {
        case STRING:
          elements = globalStringDictionarySupplier.get();
          elementOffset = 0;
          break;
        case LONG:
          elements = globalLongDictionarySupplier.get();
          elementOffset = adjustLongId;
          break;
        case DOUBLE:
          elements = globalDoubleDictionarySupplier.get();
          elementOffset = adjustDoubleId;
          break;
        default:
          throw DruidException.defensive(
              "Unhandled array type [%s] how did this happen?",
              singleType.getElementType()
          );
      }

      final int[] ids = new int[arrayToMatch.length];
      for (int i = 0; i < arrayToMatch.length; i++) {
        if (arrayToMatch[i] == null) {
          ids[i] = 0;
        } else if (singleType.getElementType().is(ValueType.STRING)) {
          ids[i] = elements.indexOf(StringUtils.toUtf8ByteBuffer((String) arrayToMatch[i]));
        } else {
          ids[i] = elements.indexOf(arrayToMatch[i]) + elementOffset;
        }
        if (ids[i] < 0) {
          if (value == null) {
            return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
          }
        }
      }

      final FixedIndexed<Integer> localDictionary = localDictionarySupplier.get();
      final FrontCodedIntArrayIndexed globalArrayDictionary = globalArrayDictionarySupplier.get();
      return new SimpleBitmapColumnIndex()
      {

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int localId = localDictionary.indexOf(globalArrayDictionary.indexOf(ids) + adjustArrayId);
          if (includeUnknown) {
            if (localId < 0) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(getBitmap(localId), nullValueBitmap)
            );
          }
          if (localId < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(localId));
        }
      };
    }
  }

  private class NestedArrayElementIndexes implements ArrayElementIndexes
  {
    private final ImmutableBitmap nullValueBitmap = localDictionarySupplier.get().get(0) == 0
                                                    ? bitmaps.get(0)
                                                    : bitmapFactory.makeEmptyImmutableBitmap();

    @Nullable
    @Override
    public BitmapColumnIndex containsValue(@Nullable Object value, TypeSignature<ValueType> elementValueType)
    {
      // this column doesn't store nested arrays, bail out if checking if we contain an array
      if (elementValueType.isArray()) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(elementValueType), value);

      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(
          eval,
          ExpressionType.fromColumnTypeStrict(singleType.isArray() ? singleType.getElementType() : singleType)
      );
      if (castForComparison == null) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final FixedIndexed<Integer> elementDictionary = arrayElementDictionarySupplier.get();
      final Indexed globalElements;
      final int elementOffset;
      switch (singleType.getElementType().getType()) {
        case STRING:
          globalElements = globalStringDictionarySupplier.get();
          elementOffset = 0;
          break;
        case LONG:
          globalElements = globalLongDictionarySupplier.get();
          elementOffset = adjustLongId;
          break;
        case DOUBLE:
          globalElements = globalDoubleDictionarySupplier.get();
          elementOffset = adjustDoubleId;
          break;
        default:
          throw DruidException.defensive(
              "Unhandled array type [%s] how did this happen?",
              singleType.getElementType()
          );
      }

      return new SimpleBitmapColumnIndex()
      {

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int elementId = getElementId();
          if (includeUnknown) {
            if (elementId < 0) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(getElementBitmap(elementId), nullValueBitmap)
            );
          }
          if (elementId < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getElementBitmap(elementId));
        }

        private int getElementId()
        {
          if (castForComparison.value() == null) {
            return 0;
          }

          if (castForComparison.type().is(ExprType.STRING)) {
            return elementDictionary.indexOf(
                globalElements.indexOf(StringUtils.toUtf8ByteBuffer(castForComparison.asString()))
            );
          } else {
            return elementDictionary.indexOf(
                globalElements.indexOf(castForComparison.value()) + elementOffset
            );
          }
        }

        private ImmutableBitmap getElementBitmap(int idx)
        {
          if (idx < 0) {
            return bitmapFactory.makeEmptyImmutableBitmap();
          }
          final ImmutableBitmap bitmap = arrayElementBitmaps.get(idx);
          return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
        }
      };
    }
  }
}
