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

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import it.unimi.dsi.fastutil.doubles.DoubleArraySet;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IntListUtils;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.CompressedColumnarDoublesSuppliers;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.index.AllFalseBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleBitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapDelegatingIterableIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.NumericRangeIndexes;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

public class ScalarDoubleColumnAndIndexSupplier implements Supplier<NestedCommonFormatColumn>, ColumnIndexSupplier
{
  public static ScalarDoubleColumnAndIndexSupplier read(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig
  )
  {
    final byte version = bb.get();
    final int columnNameLength = VByte.readInt(bb);
    final String columnName = StringUtils.fromUtf8(bb, columnNameLength);

    if (version == NestedCommonFormatColumnSerializer.V0) {
      try {

        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();

        final ByteBuffer doubleDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.DOUBLE_DICTIONARY_FILE_NAME
        );
        final ByteBuffer doublesValueColumn = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.DOUBLE_VALUE_COLUMN_FILE_NAME
        );

        final Supplier<FixedIndexed<Double>> doubleDictionarySupplier = FixedIndexed.read(
            doubleDictionaryBuffer,
            ColumnType.DOUBLE.getStrategy(),
            byteOrder,
            Double.BYTES
        );

        final Supplier<ColumnarDoubles> doubles = CompressedColumnarDoublesSuppliers.fromByteBuffer(
            doublesValueColumn,
            byteOrder
        );
        final ByteBuffer valueIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.BITMAP_INDEX_FILE_NAME
        );
        GenericIndexed<ImmutableBitmap> rBitmaps = GenericIndexed.read(
            valueIndexBuffer,
            bitmapSerdeFactory.getObjectStrategy(),
            columnBuilder.getFileMapper()
        );
        final int size;
        try (ColumnarDoubles throwAway = doubles.get()) {
          size = throwAway.size();
        }
        return new ScalarDoubleColumnAndIndexSupplier(
            doubleDictionarySupplier,
            doubles,
            rBitmaps,
            bitmapSerdeFactory.getBitmapFactory(),
            columnConfig,
            size
        );
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V%s column.", version);
      }
    } else {
      throw new RE("Unknown version " + version);
    }
  }

  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;

  private final Supplier<ColumnarDoubles> valueColumnSupplier;

  private final GenericIndexed<ImmutableBitmap> valueIndexes;

  private final BitmapFactory bitmapFactory;
  private final ImmutableBitmap nullValueBitmap;
  private final ColumnConfig columnConfig;
  private final int numRows;

  private ScalarDoubleColumnAndIndexSupplier(
      Supplier<FixedIndexed<Double>> longDictionary,
      Supplier<ColumnarDoubles> valueColumnSupplier,
      GenericIndexed<ImmutableBitmap> valueIndexes,
      BitmapFactory bitmapFactory,
      ColumnConfig columnConfig,
      int numRows
  )
  {
    this.doubleDictionarySupplier = longDictionary;
    this.valueColumnSupplier = valueColumnSupplier;
    this.valueIndexes = valueIndexes;
    this.bitmapFactory = bitmapFactory;
    this.nullValueBitmap = valueIndexes.get(0) == null ? bitmapFactory.makeEmptyImmutableBitmap() : valueIndexes.get(0);
    this.columnConfig = columnConfig;
    this.numRows = numRows;
  }

  @Override
  public NestedCommonFormatColumn get()
  {
    return new ScalarDoubleColumn(
        doubleDictionarySupplier.get(),
        valueColumnSupplier.get(),
        nullValueBitmap
    );
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      final BitmapColumnIndex nullIndex;
      if (NullHandling.replaceWithDefault()) {
        nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
      } else {
        nullIndex = new SimpleImmutableBitmapIndex(nullValueBitmap);
      }
      return (T) (NullValueIndex) () -> nullIndex;
    } else if (clazz.equals(ValueIndexes.class)) {
      return (T) new DoubleValueIndexes();
    } else if (clazz.equals(StringValueSetIndexes.class)) {
      return (T) new DoubleStringValueSetIndexes();
    } else if (clazz.equals(NumericRangeIndexes.class)) {
      return (T) new DoubleNumericRangeIndex();
    } else if (clazz.equals(DruidPredicateIndexes.class)) {
      return (T) new DoublePredicateIndexes();
    } else if (
        clazz.equals(DictionaryEncodedStringValueIndex.class) ||
        clazz.equals(DictionaryEncodedValueIndex.class)
    ) {
      return (T) new DoubleDictionaryEncodedValueSetIndex();
    }

    return null;
  }

  private ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = valueIndexes.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  private class DoubleValueIndexes implements ValueIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType)
    {
      final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(valueType), value);
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, ExpressionType.DOUBLE);
      if (castForComparison == null) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final double doubleValue = castForComparison.asDouble();
      return new SimpleBitmapColumnIndex()
      {
        final FixedIndexed<Double> dictionary = doubleDictionarySupplier.get();

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int id = dictionary.indexOf(doubleValue);
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

  private class DoubleStringValueSetIndexes implements StringValueSetIndexes
  {
    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      final boolean inputNull = value == null;
      final Double doubleValue = Strings.isNullOrEmpty(value) ? null : Doubles.tryParse(value);
      final FixedIndexed<Double> dictionary = doubleDictionarySupplier.get();
      int defaultValueIndex = dictionary.indexOf(NullHandling.defaultDoubleValue());

      return new SimpleBitmapColumnIndex()
      {

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          if (doubleValue == null) {
            if (inputNull && NullHandling.sqlCompatible()) {
              return bitmapResultFactory.wrapDimensionValue(getBitmap(0));
            } else {
              // input was not null but not a double... no match
              return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
            }
          }
          if (NullHandling.replaceWithDefault() && doubleValue.equals(NullHandling.defaultDoubleValue())) {
            if (defaultValueIndex >= 0) {
              return bitmapResultFactory.unionDimensionValueBitmaps(
                  ImmutableList.of(
                      getBitmap(0),
                      getBitmap(defaultValueIndex)
                  )
              );
            }
            return bitmapResultFactory.wrapDimensionValue(getBitmap(0));
          }
          final int id = dictionary.indexOf(doubleValue);
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
                // add null value index in default value mode
                if (NullHandling.replaceWithDefault() && theValue.equals(NullHandling.defaultDoubleValue())) {
                  needNullCheck = true;
                }
              }
            }
          }
          final boolean doNullCheck = needNullCheck;
          return () -> new Iterator<ImmutableBitmap>()
          {
            final FixedIndexed<Double> dictionary = doubleDictionarySupplier.get();
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
                next = dictionary.indexOf(nextValue);
              }
            }
          };
        }

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          if (!values.contains(null)) {
            return nullValueBitmap;
          }
          return null;
        }
      };
    }
  }

  private class DoubleNumericRangeIndex implements NumericRangeIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forRange(
        @Nullable Number startValue,
        boolean startStrict,
        @Nullable Number endValue,
        boolean endStrict
    )
    {
      final FixedIndexed<Double> dictionary = doubleDictionarySupplier.get();
      IntIntPair range = dictionary.getRange(
          startValue == null ? null : startValue.doubleValue(),
          startStrict,
          endValue == null ? null : endValue.doubleValue(),
          endStrict
      );

      final int startIndex = range.leftInt();
      final int endIndex = range.rightInt();
      if (ColumnIndexSupplier.skipComputingRangeIndexes(columnConfig, numRows, endIndex - startIndex)) {
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
          return nullValueBitmap;
        }
      };
    }
  }

  private class DoublePredicateIndexes implements DruidPredicateIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      final FixedIndexed<Double> dictionary = doubleDictionarySupplier.get();
      if (ColumnIndexSupplier.skipComputingPredicateIndexes(columnConfig, numRows, dictionary.size())) {
        return null;
      }
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable(boolean includeUnknown)
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final Iterator<Double> iterator = doubleDictionarySupplier.get().iterator();
            final DruidDoublePredicate doublePredicate = matcherFactory.makeDoublePredicate();

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
                Double nextValue = iterator.next();
                if (nextValue == null) {
                  if (NullHandling.sqlCompatible()) {
                    nextSet = doublePredicate.applyNull().matches(includeUnknown);
                  } else {
                    nextSet = doublePredicate.applyDouble(NullHandling.defaultDoubleValue()).matches(includeUnknown);
                  }
                } else {
                  nextSet = doublePredicate.applyDouble(nextValue).matches(includeUnknown);
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

  private class DoubleDictionaryEncodedValueSetIndex implements DictionaryEncodedStringValueIndex
  {
    private final FixedIndexed<Double> dictionary = doubleDictionarySupplier.get();

    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      return ScalarDoubleColumnAndIndexSupplier.this.getBitmap(idx);
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
      final Double value = dictionary.get(index);
      return value == null ? null : String.valueOf(value);
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return bitmapFactory;
    }
  }
}
