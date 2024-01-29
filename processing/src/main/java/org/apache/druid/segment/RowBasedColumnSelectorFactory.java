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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Rows;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.nested.StructuredData;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
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
  private final Supplier<T> rowSupplier;

  @Nullable
  private final RowIdSupplier rowIdSupplier;
  private final RowAdapter<T> adapter;
  private final ColumnInspector columnInspector;
  private final boolean throwParseExceptions;
  private final boolean useStringValueOfNullInLists;

  /**
   * Full constructor for {@link RowBasedCursor}. Allows passing in a rowIdSupplier, which enables
   * column value reuse optimizations.
   */
  public RowBasedColumnSelectorFactory(
      final Supplier<T> rowSupplier,
      @Nullable final RowIdSupplier rowIdSupplier,
      final RowAdapter<T> adapter,
      final ColumnInspector columnInspector,
      final boolean throwParseExceptions,
      final boolean useStringValueOfNullInLists
  )
  {
    this.rowSupplier = rowSupplier;
    this.rowIdSupplier = rowIdSupplier;
    this.adapter = adapter;
    this.columnInspector =
        Preconditions.checkNotNull(columnInspector, "columnInspector must be nonnull");
    this.throwParseExceptions = throwParseExceptions;
    this.useStringValueOfNullInLists = useStringValueOfNullInLists;
  }

  /**
   * Create an instance based on any object, along with a {@link RowAdapter} for that object.
   *
   * @param adapter                     adapter for these row objects
   * @param supplier                    supplier of row objects
   * @param columnInspector             will be used for reporting available columns and their capabilities. Note that
   *                                    this factory will still allow creation of selectors on any named field in the
   *                                    rows, even if it doesn't appear in "columnInspector". (It only needs to be
   *                                    accessible via {@link RowAdapter#columnFunction}.) As a result, you can achieve
   *                                    an untyped mode by passing in
   *                                    {@link org.apache.druid.segment.column.RowSignature#empty()}.
   * @param throwParseExceptions        whether numeric selectors should throw parse exceptions or use a default/null
   *                                    value when their inputs are not actually numeric
   * @param useStringValueOfNullInLists whether nulls in multi-value strings should be replaced with the string "null".
   *                                    for example: the list ["a", null] would be converted to ["a", "null"]. Useful
   *                                    for callers that need compatibility with {@link Rows#objectToStrings}.
   */
  public static <RowType> RowBasedColumnSelectorFactory<RowType> create(
      final RowAdapter<RowType> adapter,
      final Supplier<RowType> supplier,
      final ColumnInspector columnInspector,
      final boolean throwParseExceptions,
      final boolean useStringValueOfNullInLists
  )
  {
    return new RowBasedColumnSelectorFactory<>(
        supplier,
        null,
        adapter,
        columnInspector,
        throwParseExceptions,
        useStringValueOfNullInLists
    );
  }

  @Nullable
  static ColumnCapabilities getColumnCapabilities(
      final ColumnInspector columnInspector,
      final String columnName
  )
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
      // TIME_COLUMN_NAME is handled specially; override the provided inspector.
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
    } else {
      final ColumnCapabilities inspectedCapabilities = columnInspector.getColumnCapabilities(columnName);

      if (inspectedCapabilities != null) {
        if (inspectedCapabilities.isNumeric()) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(inspectedCapabilities);
        }

        if (inspectedCapabilities.isArray()) {
          return ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(inspectedCapabilities);
        }

        // Do _not_ set isDictionaryEncoded or hasBitmapIndexes, because Row-based columns do not have those things.
        final ColumnCapabilitiesImpl retVal = new ColumnCapabilitiesImpl()
            .setType(inspectedCapabilities)
            .setDictionaryValuesUnique(false)
            .setDictionaryValuesSorted(false);

        // Set hasMultipleValues = false if the inspector asserts that there will not be multiple values.
        //
        // Note: we do not set hasMultipleValues = true ever, because even though we might return multiple values,
        // setting it affirmatively causes expression selectors to always treat the column values as arrays. And we
        // don't want that.
        if (inspectedCapabilities.hasMultipleValues().isFalse()) {
          retVal.setHasMultipleValues(false);
        }

        return retVal;
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
        private long currentId = RowIdSupplier.INIT;
        private String currentValue;

        @Override
        protected String getValue()
        {
          updateCurrentValue();
          return currentValue;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", rowSupplier);
          inspector.visit("extractionFn", extractionFn);
        }

        private void updateCurrentValue()
        {
          if (rowIdSupplier == null || rowIdSupplier.getRowId() != currentId) {
            currentValue = extractionFn.apply(timestampFunction.applyAsLong(rowSupplier.get()));

            if (rowIdSupplier != null) {
              currentId = rowIdSupplier.getRowId();
            }
          }
        }
      };
    } else {
      final Function<T, Object> dimFunction = adapter.columnFunction(dimension);

      return new DimensionSelector()
      {
        private long currentId = RowIdSupplier.INIT;
        private List<String> dimensionValues;

        private final RangeIndexedInts indexedInts = new RangeIndexedInts();

        @Override
        public IndexedInts getRow()
        {
          updateCurrentValues();
          indexedInts.setSize(dimensionValues.size());
          return indexedInts;
        }

        @Override
        public ValueMatcher makeValueMatcher(final @Nullable String value)
        {
          return new ValueMatcher()
          {
            @Override
            public boolean matches(boolean includeUnknown)
            {
              updateCurrentValues();

              if (dimensionValues.isEmpty()) {
                return includeUnknown || value == null;
              }

              for (String dimensionValue : dimensionValues) {
                final String coerced = NullHandling.emptyToNullIfNeeded(dimensionValue);
                if ((includeUnknown && coerced == null) || Objects.equals(coerced, value)) {
                  return true;
                }
              }
              return false;
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("row", rowSupplier);
              inspector.visit("extractionFn", extractionFn);
            }
          };
        }

        @Override
        public ValueMatcher makeValueMatcher(final DruidPredicateFactory predicateFactory)
        {
          final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();

          return new ValueMatcher()
          {
            @Override
            public boolean matches(boolean includeUnknown)
            {
              updateCurrentValues();

              if (dimensionValues.isEmpty()) {
                return predicate.apply(null).matches(includeUnknown);
              }

              for (String dimensionValue : dimensionValues) {
                final String coerced = NullHandling.emptyToNullIfNeeded(dimensionValue);
                if (predicate.apply(coerced).matches(includeUnknown)) {
                  return true;
                }
              }
              return false;
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("row", rowSupplier);
              inspector.visit("predicate", predicateFactory);
              inspector.visit("extractionFn", extractionFn);
            }
          };
        }

        @Override
        public int getValueCardinality()
        {
          return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public String lookupName(int id)
        {
          updateCurrentValues();
          return NullHandling.emptyToNullIfNeeded(dimensionValues.get(id));
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
          updateCurrentValues();

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
          inspector.visit("row", rowSupplier);
          inspector.visit("extractionFn", extractionFn);
        }

        private void updateCurrentValues()
        {
          if (rowIdSupplier == null || rowIdSupplier.getRowId() != currentId) {
            try {
              final Object rawValue = dimFunction.apply(rowSupplier.get());

              if (rawValue == null || rawValue instanceof String) {
                final String s = NullHandling.emptyToNullIfNeeded((String) rawValue);

                if (extractionFn == null) {
                  dimensionValues = Collections.singletonList(s);
                } else {
                  dimensionValues = Collections.singletonList(extractionFn.apply(s));
                }
              } else if (rawValue instanceof List) {
                //noinspection rawtypes
                final List<String> values = new ArrayList<>(((List) rawValue).size());

                //noinspection rawtypes
                for (final Object item : ((List) rawValue)) {
                  final String itemString;

                  if (useStringValueOfNullInLists) {
                    itemString = String.valueOf(item);
                  } else {
                    itemString = item == null ? null : String.valueOf(item);
                  }

                  // Behavior with null item is to convert it to string "null". This is not what most other areas of Druid
                  // would do when treating a null as a string, but it's consistent with Rows.objectToStrings, which is
                  // commonly used when retrieving strings from input-row-like objects.
                  if (extractionFn == null) {
                    values.add(itemString);
                  } else {
                    values.add(extractionFn.apply(itemString));
                  }
                }

                dimensionValues = values;
              } else {
                final List<String> nonExtractedValues = Rows.objectToStrings(rawValue);
                dimensionValues = new ArrayList<>(nonExtractedValues.size());

                for (final String value : nonExtractedValues) {
                  final String s = NullHandling.emptyToNullIfNeeded(value);

                  if (extractionFn == null) {
                    dimensionValues.add(s);
                  } else {
                    dimensionValues.add(extractionFn.apply(s));
                  }
                }
              }
            }
            catch (Throwable e) {
              currentId = RowIdSupplier.INIT;
              throw e;
            }

            if (rowIdSupplier != null) {
              currentId = rowIdSupplier.getRowId();
            }
          }
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
          return timestampFunction.applyAsLong(rowSupplier.get());
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
          inspector.visit("row", rowSupplier);
        }
      }
      return new TimeLongColumnSelector();
    } else {
      final Function<T, Object> columnFunction = adapter.columnFunction(columnName);
      final ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(columnName);
      final ValueType numberType =
          capabilities != null && capabilities.getType().isNumeric() ? capabilities.getType() : null;

      return new ColumnValueSelector<Object>()
      {
        private long currentValueId = RowIdSupplier.INIT;
        private long currentValueAsNumberId = RowIdSupplier.INIT;
        @Nullable
        private Object currentValue;
        @Nullable
        private Number currentValueAsNumber;

        @Override
        public boolean isNull()
        {
          updateCurrentValueAsNumber();
          return !NullHandling.replaceWithDefault() && currentValueAsNumber == null;
        }

        @Override
        public double getDouble()
        {
          updateCurrentValueAsNumber();
          assert NullHandling.replaceWithDefault() || currentValueAsNumber != null;
          return currentValueAsNumber != null ? currentValueAsNumber.doubleValue() : 0d;
        }

        @Override
        public float getFloat()
        {
          updateCurrentValueAsNumber();
          assert NullHandling.replaceWithDefault() || currentValueAsNumber != null;
          return currentValueAsNumber != null ? currentValueAsNumber.floatValue() : 0f;
        }

        @Override
        public long getLong()
        {
          updateCurrentValueAsNumber();
          assert NullHandling.replaceWithDefault() || currentValueAsNumber != null;
          return currentValueAsNumber != null ? currentValueAsNumber.longValue() : 0L;
        }

        @Nullable
        @Override
        public Object getObject()
        {
          updateCurrentValue();
          return currentValue;
        }

        @Override
        public Class<Object> classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", rowSupplier);
        }

        private void updateCurrentValue()
        {
          if (rowIdSupplier == null || rowIdSupplier.getRowId() != currentValueId) {
            try {
              currentValue = columnFunction.apply(rowSupplier.get());
            }
            catch (Throwable e) {
              currentValueId = RowIdSupplier.INIT;
              throw e;
            }

            if (rowIdSupplier != null) {
              currentValueId = rowIdSupplier.getRowId();
            }
          }
        }

        private void updateCurrentValueAsNumber()
        {
          updateCurrentValue();

          if (rowIdSupplier == null || rowIdSupplier.getRowId() != currentValueAsNumberId) {
            try {
              final Object valueToUse =
                  currentValue instanceof StructuredData ? ((StructuredData) currentValue).getValue() : currentValue;
              currentValueAsNumber = Rows.objectToNumber(columnName, valueToUse, numberType, throwParseExceptions);
            }
            catch (Throwable e) {
              currentValueAsNumberId = RowIdSupplier.INIT;
              throw e;
            }

            if (rowIdSupplier != null) {
              currentValueAsNumberId = rowIdSupplier.getRowId();
            }
          }
        }
      };
    }
  }

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return rowIdSupplier;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return getColumnCapabilities(columnInspector, columnName);
  }
}
