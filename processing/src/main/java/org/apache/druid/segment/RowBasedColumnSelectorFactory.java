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

package org.apache.druid.segment;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Rows;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * A {@link ColumnSelectorFactory} that is based on an object supplier and a {@link RowAdapter} for that type of object.
 */
public class RowBasedColumnSelectorFactory<T> implements ColumnSelectorFactory
{
  private final Supplier<T> supplier;
  private final RowAdapter<T> adapter;
  private final RowSignature rowSignature;
  private final boolean throwParseExceptions;

  private RowBasedColumnSelectorFactory(
      final Supplier<T> supplier,
      final RowAdapter<T> adapter,
      final RowSignature rowSignature,
      final boolean throwParseExceptions
  )
  {
    this.supplier = supplier;
    this.adapter = adapter;
    this.rowSignature = Preconditions.checkNotNull(rowSignature, "rowSignature must be nonnull");
    this.throwParseExceptions = throwParseExceptions;
  }

  /**
   * Create an instance based on any object, along with a {@link RowAdapter} for that object.
   *
   * @param adapter              adapter for these row objects
   * @param supplier             supplier of row objects
   * @param signature            will be used for reporting available columns and their capabilities. Note that the this
   *                             factory will still allow creation of selectors on any named field in the rows, even if
   *                             it doesn't appear in "rowSignature". (It only needs to be accessible via
   *                             {@link RowAdapter#columnFunction}.) As a result, you can achieve an untyped mode by
   *                             passing in {@link RowSignature#empty()}.
   * @param throwParseExceptions whether numeric selectors should throw parse exceptions or use a default/null value
   *                             when their inputs are not actually numeric
   */
  public static <RowType> RowBasedColumnSelectorFactory<RowType> create(
      final RowAdapter<RowType> adapter,
      final Supplier<RowType> supplier,
      final RowSignature signature,
      final boolean throwParseExceptions
  )
  {
    return new RowBasedColumnSelectorFactory<>(supplier, adapter, signature, throwParseExceptions);
  }

  @Nullable
  static ColumnCapabilities getColumnCapabilities(
      final RowSignature rowSignature,
      final String columnName
  )
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
      // TIME_COLUMN_NAME is handled specially; override the provided rowSignature.
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.LONG);
    } else {
      final ValueType valueType = rowSignature.getColumnType(columnName).orElse(null);

      // Do _not_ set isDictionaryEncoded or hasBitmapIndexes, because Row-based columns do not have those things.
      // Do not set hasMultipleValues, because even though we might return multiple values, setting it affirmatively
      // causes expression selectors to always treat us as arrays. If we might have multiple values (i.e. if our type
      // is nonnumeric), set isComplete false to compensate.
      if (valueType != null) {
        if (valueType.isNumeric()) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(valueType);
        }
        return new ColumnCapabilitiesImpl()
            .setType(valueType)
            .setDictionaryValuesUnique(false)
            .setDictionaryValuesSorted(false);
      } else {
        return null;
      }
    }
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    // This dimension selector does not have an associated lookup dictionary, which means lookup can only be done
    // on the same row. Hence it returns CARDINALITY_UNKNOWN from getValueCardinality.
    return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
  }

  private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
  {
    final String dimension = dimensionSpec.getDimension();
    final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

    if (ColumnHolder.TIME_COLUMN_NAME.equals(dimensionSpec.getDimension())) {
      if (extractionFn == null) {
        throw new UnsupportedOperationException("time dimension must provide an extraction function");
      }

      final ToLongFunction<T> timestampFunction = adapter.timestampFunction();

      return new BaseSingleValueDimensionSelector()
      {
        @Override
        protected String getValue()
        {
          return extractionFn.apply(timestampFunction.applyAsLong(supplier.get()));
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", supplier);
          inspector.visit("extractionFn", extractionFn);
        }
      };
    } else {
      final Function<T, Object> dimFunction = adapter.columnFunction(dimension);

      return new DimensionSelector()
      {
        private final RangeIndexedInts indexedInts = new RangeIndexedInts();

        @Override
        public IndexedInts getRow()
        {
          final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
          indexedInts.setSize(dimensionValues != null ? dimensionValues.size() : 0);
          return indexedInts;
        }

        @Override
        public ValueMatcher makeValueMatcher(final @Nullable String value)
        {
          if (extractionFn == null) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
                if (dimensionValues == null || dimensionValues.isEmpty()) {
                  return value == null;
                }

                for (String dimensionValue : dimensionValues) {
                  if (Objects.equals(NullHandling.emptyToNullIfNeeded(dimensionValue), value)) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("row", supplier);
              }
            };
          } else {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
                if (dimensionValues == null || dimensionValues.isEmpty()) {
                  return value == null;
                }

                for (String dimensionValue : dimensionValues) {
                  if (Objects.equals(extractionFn.apply(NullHandling.emptyToNullIfNeeded(dimensionValue)), value)) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("row", supplier);
                inspector.visit("extractionFn", extractionFn);
              }
            };
          }
        }

        @Override
        public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
        {
          final boolean matchNull = predicate.apply(null);
          if (extractionFn == null) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
                if (dimensionValues == null || dimensionValues.isEmpty()) {
                  return matchNull;
                }

                for (String dimensionValue : dimensionValues) {
                  if (predicate.apply(NullHandling.emptyToNullIfNeeded(dimensionValue))) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("row", supplier);
                inspector.visit("predicate", predicate);
              }
            };
          } else {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
                if (dimensionValues == null || dimensionValues.isEmpty()) {
                  return matchNull;
                }

                for (String dimensionValue : dimensionValues) {
                  if (predicate.apply(extractionFn.apply(NullHandling.emptyToNullIfNeeded(dimensionValue)))) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("row", supplier);
                inspector.visit("predicate", predicate);
              }
            };
          }
        }

        @Override
        public int getValueCardinality()
        {
          return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public String lookupName(int id)
        {
          final String value = NullHandling.emptyToNullIfNeeded(
              Rows.objectToStrings(dimFunction.apply(supplier.get())).get(id)
          );
          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public boolean nameLookupPossibleInAdvance()
        {
          return false;
        }

        @Nullable
        @Override
        public IdLookup idLookup()
        {
          return null;
        }

        @Nullable
        @Override
        public Object getObject()
        {
          List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
          if (dimensionValues == null) {
            return null;
          }
          if (dimensionValues.size() == 1) {
            return dimensionValues.get(0);
          }
          return dimensionValues;
        }

        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", supplier);
          inspector.visit("extractionFn", extractionFn);
        }
      };
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (columnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      final ToLongFunction<T> timestampFunction = adapter.timestampFunction();

      class TimeLongColumnSelector implements LongColumnSelector
      {
        @Override
        public long getLong()
        {
          return timestampFunction.applyAsLong(supplier.get());
        }

        @Override
        public boolean isNull()
        {
          // Time column never has null values
          return false;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", supplier);
        }
      }
      return new TimeLongColumnSelector();
    } else {
      final Function<T, Object> columnFunction = adapter.columnFunction(columnName);

      return new ColumnValueSelector<Object>()
      {
        @Override
        public boolean isNull()
        {
          return !NullHandling.replaceWithDefault() && getCurrentValueAsNumber() == null;
        }

        @Override
        public double getDouble()
        {
          final Number n = getCurrentValueAsNumber();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.doubleValue() : 0d;
        }

        @Override
        public float getFloat()
        {
          final Number n = getCurrentValueAsNumber();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.floatValue() : 0f;
        }

        @Override
        public long getLong()
        {
          final Number n = getCurrentValueAsNumber();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.longValue() : 0L;
        }

        @Nullable
        @Override
        public Object getObject()
        {
          return getCurrentValue();
        }

        @Override
        public Class<Object> classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", supplier);
        }

        @Nullable
        private Object getCurrentValue()
        {
          return columnFunction.apply(supplier.get());
        }

        @Nullable
        private Number getCurrentValueAsNumber()
        {
          return Rows.objectToNumber(
              columnName,
              getCurrentValue(),
              throwParseExceptions
          );
        }
      };
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return getColumnCapabilities(rowSignature, columnName);
  }
}
