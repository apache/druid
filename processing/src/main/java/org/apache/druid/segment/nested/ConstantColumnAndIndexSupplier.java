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
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.column.NumericRangeIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.VByte;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.SortedSet;

public class ConstantColumnAndIndexSupplier implements Supplier<NestedCommonFormatColumn>, ColumnIndexSupplier
{
  public static ConstantColumnAndIndexSupplier read(
      ColumnType logicalType,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteBuffer bb
  )
  {
    final byte version = bb.get();
    final int columnNameLength = VByte.readInt(bb);
    final String columnName = StringUtils.fromUtf8(bb, columnNameLength);

    if (version == NestedCommonFormatColumnSerializer.V0) {
      try {
        final int rowCount = VByte.readInt(bb);
        final int valueLength = VByte.readInt(bb);
        final Object constantValue = NestedDataComplexTypeSerde.OBJECT_MAPPER.readValue(
            IndexMerger.SERIALIZER_UTILS.readBytes(bb, valueLength),
            Object.class
        );
        return new ConstantColumnAndIndexSupplier(logicalType, bitmapSerdeFactory, rowCount, constantValue);
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V%s column [%s].", version, columnName);
      }
    } else {
      throw new RE("Unknown version " + version);
    }
  }

  private final ColumnType logicalType;
  private final BitmapSerdeFactory bitmapSerdeFactory;
  @Nullable
  private final Object constantValue;

  private final ImmutableBitmap matchBitmap;
  private final SimpleImmutableBitmapIndex match;
  private final ImmutableBitmap noMatchBitmap;
  private final SimpleImmutableBitmapIndex noMatch;

  public ConstantColumnAndIndexSupplier(
      ColumnType logicalType,
      BitmapSerdeFactory bitmapSerdeFactory,
      int numRows,
      @Nullable Object constantValue
  )
  {
    this.logicalType = logicalType;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.constantValue = constantValue;
    this.matchBitmap = bitmapSerdeFactory.getBitmapFactory().complement(
        bitmapSerdeFactory.getBitmapFactory().makeEmptyImmutableBitmap(),
        numRows
    );
    this.match = new SimpleImmutableBitmapIndex(matchBitmap);
    this.noMatchBitmap = bitmapSerdeFactory.getBitmapFactory().makeEmptyImmutableBitmap();
    this.noMatch = new SimpleImmutableBitmapIndex(noMatchBitmap);
  }

  @Override
  public NestedCommonFormatColumn get()
  {
    return new ConstantColumn(logicalType, constantValue);
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      final BitmapColumnIndex nullIndex;
      if (constantValue == null) {
        nullIndex = match;
      } else {
        nullIndex = noMatch;
      }
      return (T) (NullValueIndex) () -> nullIndex;
    } else if (logicalType.isPrimitive() && (clazz.equals(DictionaryEncodedStringValueIndex.class)
             || clazz.equals(DictionaryEncodedValueIndex.class))) {
      return (T) new ConstantDictionaryEncodedStringValueIndex();
    } else if (logicalType.isPrimitive() && clazz.equals(StringValueSetIndex.class)) {
      return (T) new ConstantStringValueSetIndex();
    } else if (logicalType.is(ValueType.STRING) && clazz.equals(LexicographicalRangeIndex.class)) {
      return (T) new ConstantLexicographicalRangeIndex();
    } else if (logicalType.isNumeric() && clazz.equals(NumericRangeIndex.class)) {
      return (T) new ConstantNumericRangeIndex();
    } else if (logicalType.isPrimitive() && clazz.equals(DruidPredicateIndex.class)) {
      return (T) new ConstantDruidPredicateIndex();
    }
    return null;
  }

  class ConstantStringValueSetIndex implements StringValueSetIndex
  {
    private final String theString = Evals.asString(constantValue);

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      if (Objects.equals(value, theString)) {
        return match;
      }
      return noMatch;
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      if (values.contains(theString)) {
        return match;
      }
      return noMatch;
    }
  }

  class ConstantLexicographicalRangeIndex implements LexicographicalRangeIndex
  {
    private final String theString = NullHandling.emptyToNullIfNeeded(Evals.asString(constantValue));
    private final NullableTypeStrategy<String> strategy = logicalType.getNullableStrategy();

    @Nullable
    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      if (inRange(startValue, startStrict, endValue, endStrict)) {
        return match;
      }
      return noMatch;
    }

    @Nullable
    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict,
        Predicate<String> matcher
    )
    {
      if (inRange(startValue, startStrict, endValue, endStrict) && matcher.apply(theString)) {
        return match;
      }
      return noMatch;
    }

    private boolean inRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      // a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
      final int startCompare = strategy.compare(NullHandling.emptyToNullIfNeeded(startValue), theString);
      final int endCompare = endValue == null ? -1 : strategy.compare(theString, NullHandling.emptyToNullIfNeeded(endValue));
      final boolean startSatisfied = startStrict ? startCompare < 0 : startCompare <= 0;
      final boolean endSatisified = endStrict ? endCompare < 0 : endCompare <= 0;
      return startSatisfied && endSatisified;
    }
  }

  class ConstantNumericRangeIndex implements NumericRangeIndex
  {
    private final NullableTypeStrategy<Number> strategy = logicalType.getNullableStrategy();

    @Nullable
    @Override
    public BitmapColumnIndex forRange(
        @Nullable Number startValue,
        boolean startStrict,
        @Nullable Number endValue,
        boolean endStrict
    )
    {
      if (constantValue == null) {
        return noMatch;
      }
      final Number theNumber = (Number) constantValue;
      final int startCompare = strategy.compare(startValue, theNumber);
      final int endCompare = endValue == null ? -1 : strategy.compare(theNumber, endValue);
      final boolean startSatisfied = startStrict ? startCompare < 0 : startCompare <= 0;
      final boolean endSatisified = endStrict ? endCompare < 0 : endCompare <= 0;
      if (startSatisfied && endSatisified) {
        return match;
      }
      return noMatch;
    }
  }

  class ConstantDruidPredicateIndex implements DruidPredicateIndex
  {
    @Nullable
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      switch (logicalType.getType()) {
        case STRING:
          if (matcherFactory.makeStringPredicate().apply((String) constantValue)) {
            return match;
          }
          return noMatch;
        case LONG:
          DruidLongPredicate longPredicate = matcherFactory.makeLongPredicate();
          if (constantValue == null) {
            if (longPredicate.applyNull()) {
              return match;
            }
            return noMatch;
          } else if (longPredicate.applyLong(((Number) constantValue).longValue())) {
            return match;
          }
          return noMatch;
        case FLOAT:
          DruidFloatPredicate floatPredicate = matcherFactory.makeFloatPredicate();
          if (constantValue == null) {
            if (floatPredicate.applyNull()) {
              return match;
            }
            return noMatch;
          } else if (floatPredicate.applyFloat(((Number) constantValue).floatValue())) {
            return match;
          }
          return noMatch;
        case DOUBLE:
          DruidDoublePredicate doublePredicate = matcherFactory.makeDoublePredicate();
          if (constantValue == null) {
            if (doublePredicate.applyNull()) {
              return match;
            }
            return noMatch;
          } else if (doublePredicate.applyDouble(((Number) constantValue).doubleValue())) {
            return match;
          }
          return noMatch;
        default:
          if (matcherFactory.makeObjectPredicate().apply(constantValue)) {
            return match;
          }
          return noMatch;
      }
    }
  }

  class ConstantDictionaryEncodedStringValueIndex implements DictionaryEncodedStringValueIndex
  {
    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      if (idx == 0) {
        return matchBitmap;
      }
      return noMatchBitmap;
    }

    @Override
    public int getCardinality()
    {
      return 1;
    }

    @Nullable
    @Override
    public String getValue(int index)
    {
      if (index == 0) {
        return Evals.asString(constantValue);
      }
      return null;
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return bitmapSerdeFactory.getBitmapFactory();
    }
  }
}
