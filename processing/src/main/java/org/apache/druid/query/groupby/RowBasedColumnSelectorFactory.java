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

package org.apache.druid.query.groupby;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Row;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RowBasedColumnSelectorFactory implements ColumnSelectorFactory
{
  private final Supplier<? extends Row> row;
  private final Map<String, ValueType> rowSignature;

  private RowBasedColumnSelectorFactory(
      final Supplier<? extends Row> row,
      @Nullable final Map<String, ValueType> rowSignature
  )
  {
    this.row = row;
    this.rowSignature = rowSignature != null ? rowSignature : ImmutableMap.of();
  }

  public static RowBasedColumnSelectorFactory create(
      final Supplier<? extends Row> row,
      @Nullable final Map<String, ValueType> rowSignature
  )
  {
    return new RowBasedColumnSelectorFactory(row, rowSignature);
  }

  public static RowBasedColumnSelectorFactory create(
      final ThreadLocal<? extends Row> row,
      @Nullable final Map<String, ValueType> rowSignature
  )
  {
    return new RowBasedColumnSelectorFactory(row::get, rowSignature);
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

      return new BaseSingleValueDimensionSelector()
      {
        @Override
        protected String getValue()
        {
          return extractionFn.apply(row.get().getTimestampFromEpoch());
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", row);
          inspector.visit("extractionFn", extractionFn);
        }
      };
    } else {
      return new DimensionSelector()
      {
        private final RangeIndexedInts indexedInts = new RangeIndexedInts();

        @Override
        public IndexedInts getRow()
        {
          final List<String> dimensionValues = row.get().getDimension(dimension);
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
                final List<String> dimensionValues = row.get().getDimension(dimension);
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
                inspector.visit("row", row);
              }
            };
          } else {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                final List<String> dimensionValues = row.get().getDimension(dimension);
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
                inspector.visit("row", row);
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
                final List<String> dimensionValues = row.get().getDimension(dimension);
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
                inspector.visit("row", row);
                inspector.visit("predicate", predicate);
              }
            };
          } else {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                final List<String> dimensionValues = row.get().getDimension(dimension);
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
                inspector.visit("row", row);
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
          final String value = NullHandling.emptyToNullIfNeeded(row.get().getDimension(dimension).get(id));
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
          List<String> dimensionValues = row.get().getDimension(dimension);
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
          inspector.visit("row", row);
          inspector.visit("extractionFn", extractionFn);
        }
      };
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (columnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      class TimeLongColumnSelector implements LongColumnSelector
      {
        @Override
        public long getLong()
        {
          return row.get().getTimestampFromEpoch();
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
          inspector.visit("row", row);
        }
      }
      return new TimeLongColumnSelector();
    } else {
      return new ColumnValueSelector()
      {
        @Override
        public boolean isNull()
        {
          return row.get().getRaw(columnName) == null;
        }

        @Override
        public double getDouble()
        {
          Number metric = row.get().getMetric(columnName);
          assert NullHandling.replaceWithDefault() || metric != null;
          return DimensionHandlerUtils.nullToZero(metric).doubleValue();
        }

        @Override
        public float getFloat()
        {
          Number metric = row.get().getMetric(columnName);
          assert NullHandling.replaceWithDefault() || metric != null;
          return DimensionHandlerUtils.nullToZero(metric).floatValue();
        }

        @Override
        public long getLong()
        {
          Number metric = row.get().getMetric(columnName);
          assert NullHandling.replaceWithDefault() || metric != null;
          return DimensionHandlerUtils.nullToZero(metric).longValue();
        }

        @Nullable
        @Override
        public Object getObject()
        {
          return row.get().getRaw(columnName);
        }

        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", row);
        }
      };
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
      // TIME_COLUMN_NAME is handled specially; override the provided rowSignature.
      return new ColumnCapabilitiesImpl().setType(ValueType.LONG);
    } else {
      final ValueType valueType = rowSignature.get(columnName);

      // Do _not_ set isDictionaryEncoded or hasBitmapIndexes, because Row-based columns do not have those things.
      return valueType != null ? new ColumnCapabilitiesImpl().setType(valueType) : null;
    }
  }
}
