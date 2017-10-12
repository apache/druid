/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.java.util.common.io.Closer;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Map;

/**
 * The basic implementation of {@link ColumnSelectorFactory} over a historical segment (i. e. {@link QueryableIndex}).
 * It's counterpart for incremental index is {@link io.druid.segment.incremental.IncrementalIndexColumnSelectorFactory}.
 */
class QueryableIndexColumnSelectorFactory implements ColumnSelectorFactory
{
  private final QueryableIndex index;
  private final VirtualColumns virtualColumns;
  private final boolean descending;
  private final Closer closer;
  protected final ReadableOffset offset;

  private final Map<String, DictionaryEncodedColumn> dictionaryColumnCache;
  private final Map<String, GenericColumn> genericColumnCache;
  private final Map<String, Object> objectColumnCache;

  QueryableIndexColumnSelectorFactory(
      QueryableIndex index,
      VirtualColumns virtualColumns,
      boolean descending,
      Closer closer,
      ReadableOffset offset,
      Map<String, DictionaryEncodedColumn> dictionaryColumnCache,
      Map<String, GenericColumn> genericColumnCache,
      Map<String, Object> objectColumnCache
  )
  {
    this.index = index;
    this.virtualColumns = virtualColumns;
    this.descending = descending;
    this.closer = closer;
    this.offset = offset;
    this.dictionaryColumnCache = dictionaryColumnCache;
    this.genericColumnCache = genericColumnCache;
    this.objectColumnCache = objectColumnCache;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    if (virtualColumns.exists(dimensionSpec.getDimension())) {
      return virtualColumns.makeDimensionSelector(dimensionSpec, this);
    }

    return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
  }

  private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
  {
    final String dimension = dimensionSpec.getDimension();
    final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

    final Column columnDesc = index.getColumn(dimension);
    if (columnDesc == null) {
      return DimensionSelectorUtils.constantSelector(null, extractionFn);
    }

    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return new SingleScanTimeDimSelector(
          makeLongColumnSelector(dimension),
          extractionFn,
          descending
      );
    }

    if (columnDesc.getCapabilities().getType() == ValueType.LONG) {
      return new LongWrappingDimensionSelector(makeLongColumnSelector(dimension), extractionFn);
    }

    if (columnDesc.getCapabilities().getType() == ValueType.FLOAT) {
      return new FloatWrappingDimensionSelector(makeFloatColumnSelector(dimension), extractionFn);
    }

    if (columnDesc.getCapabilities().getType() == ValueType.DOUBLE) {
      return new DoubleWrappingDimensionSelector(makeDoubleColumnSelector(dimension), extractionFn);
    }

    DictionaryEncodedColumn<String> cachedColumn = dictionaryColumnCache.get(dimension);
    if (cachedColumn == null) {
      cachedColumn = columnDesc.getDictionaryEncoding();
      closer.register(cachedColumn);
      dictionaryColumnCache.put(dimension, cachedColumn);
    }

    final DictionaryEncodedColumn<String> column = cachedColumn;
    if (column == null) {
      return DimensionSelectorUtils.constantSelector(null, extractionFn);
    } else {
      return column.makeDimensionSelector(offset, extractionFn);
    }
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeFloatColumnSelector(columnName, this);
    }

    GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

    if (cachedMetricVals == null) {
      Column holder = index.getColumn(columnName);
      if (holder != null && ValueType.isNumeric(holder.getCapabilities().getType())) {
        cachedMetricVals = holder.getGenericColumn();
        closer.register(cachedMetricVals);
        genericColumnCache.put(columnName, cachedMetricVals);
      }
    }

    if (cachedMetricVals == null) {
      return ZeroFloatColumnSelector.instance();
    }

    return cachedMetricVals.makeFloatSingleValueRowSelector(offset);
  }

  @Override
  public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeDoubleColumnSelector(columnName, this);
    }

    GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

    if (cachedMetricVals == null) {
      Column holder = index.getColumn(columnName);
      if (holder != null && ValueType.isNumeric(holder.getCapabilities().getType())) {
        cachedMetricVals = holder.getGenericColumn();
        closer.register(cachedMetricVals);
        genericColumnCache.put(columnName, cachedMetricVals);
      }
    }

    if (cachedMetricVals == null) {
      return ZeroDoubleColumnSelector.instance();
    }

    return cachedMetricVals.makeDoubleSingleValueRowSelector(offset);
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeLongColumnSelector(columnName, this);
    }

    GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

    if (cachedMetricVals == null) {
      Column holder = index.getColumn(columnName);
      if (holder != null && ValueType.isNumeric(holder.getCapabilities().getType())) {
        cachedMetricVals = holder.getGenericColumn();
        closer.register(cachedMetricVals);
        genericColumnCache.put(columnName, cachedMetricVals);
      }
    }

    if (cachedMetricVals == null) {
      return ZeroLongColumnSelector.instance();
    }

    return cachedMetricVals.makeLongSingleValueRowSelector(offset);
  }

  @Nullable
  @Override
  public ObjectColumnSelector makeObjectColumnSelector(String column)
  {
    if (virtualColumns.exists(column)) {
      return virtualColumns.makeObjectColumnSelector(column, this);
    }

    Object cachedColumnVals = objectColumnCache.get(column);

    if (cachedColumnVals == null) {
      Column holder = index.getColumn(column);

      if (holder != null) {
        final ColumnCapabilities capabilities = holder.getCapabilities();

        if (capabilities.isDictionaryEncoded()) {
          cachedColumnVals = holder.getDictionaryEncoding();
        } else if (capabilities.getType() == ValueType.COMPLEX) {
          cachedColumnVals = holder.getComplexColumn();
        } else {
          cachedColumnVals = holder.getGenericColumn();
        }
      }

      if (cachedColumnVals != null) {
        closer.register((Closeable) cachedColumnVals);
        objectColumnCache.put(column, cachedColumnVals);
      }
    }

    if (cachedColumnVals == null) {
      return null;
    }

    if (cachedColumnVals instanceof GenericColumn) {
      final GenericColumn columnVals = (GenericColumn) cachedColumnVals;
      final ValueType type = columnVals.getType();

      if (columnVals.hasMultipleValues()) {
        throw new UnsupportedOperationException(
            "makeObjectColumnSelector does not support multi-value GenericColumns"
        );
      }

      if (type == ValueType.FLOAT) {
        return new ObjectColumnSelector<Float>()
        {
          @Override
          public Class<Float> classOfObject()
          {
            return Float.class;
          }

          @Override
          public Float getObject()
          {
            return columnVals.getFloatSingleValueRow(offset.getOffset());
          }
        };
      }
      if (type == ValueType.DOUBLE) {
        return new ObjectColumnSelector<Double>()
        {
          @Override
          public Class<Double> classOfObject()
          {
            return Double.class;
          }

          @Override
          public Double getObject()
          {
            return columnVals.getDoubleSingleValueRow(offset.getOffset());
          }
        };
      }
      if (type == ValueType.LONG) {
        return new ObjectColumnSelector<Long>()
        {
          @Override
          public Class<Long> classOfObject()
          {
            return Long.class;
          }

          @Override
          public Long getObject()
          {
            return columnVals.getLongSingleValueRow(offset.getOffset());
          }
        };
      }
      if (type == ValueType.STRING) {
        return new ObjectColumnSelector<String>()
        {
          @Override
          public Class<String> classOfObject()
          {
            return String.class;
          }

          @Override
          public String getObject()
          {
            return columnVals.getStringSingleValueRow(offset.getOffset());
          }
        };
      }
    }

    if (cachedColumnVals instanceof DictionaryEncodedColumn) {
      final DictionaryEncodedColumn<String> columnVals = (DictionaryEncodedColumn) cachedColumnVals;
      if (columnVals.hasMultipleValues()) {
        return new ObjectColumnSelector<Object>()
        {
          @Override
          public Class<Object> classOfObject()
          {
            return Object.class;
          }

          @Override
          @Nullable
          public Object getObject()
          {
            final IndexedInts multiValueRow = columnVals.getMultiValueRow(offset.getOffset());
            if (multiValueRow.size() == 0) {
              return null;
            } else if (multiValueRow.size() == 1) {
              return columnVals.lookupName(multiValueRow.get(0));
            } else {
              final String[] strings = new String[multiValueRow.size()];
              for (int i = 0; i < multiValueRow.size(); i++) {
                strings[i] = columnVals.lookupName(multiValueRow.get(i));
              }
              return strings;
            }
          }
        };
      } else {
        return new ObjectColumnSelector<String>()
        {
          @Override
          public Class<String> classOfObject()
          {
            return String.class;
          }

          @Override
          public String getObject()
          {
            return columnVals.lookupName(columnVals.getSingleValueRow(offset.getOffset()));
          }
        };
      }
    }

    final ComplexColumn columnVals = (ComplexColumn) cachedColumnVals;
    return new ObjectColumnSelector()
    {
      @Override
      public Class classOfObject()
      {
        return columnVals.getClazz();
      }

      @Override
      public Object getObject()
      {
        return columnVals.getRowValue(offset.getOffset());
      }
    };
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.getColumnCapabilities(columnName);
    }

    return QueryableIndexStorageAdapter.getColumnCapabilites(index, columnName);
  }
}
