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

package io.druid.segment.incremental;

import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelectorUtils;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.DoubleWrappingDimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.FloatWrappingDimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.LongWrappingDimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.SingleScanTimeDimSelector;
import io.druid.segment.VirtualColumns;
import io.druid.segment.ZeroDoubleColumnSelector;
import io.druid.segment.ZeroFloatColumnSelector;
import io.druid.segment.ZeroLongColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

import javax.annotation.Nullable;

/**
 * The basic implementation of {@link ColumnSelectorFactory} over an {@link IncrementalIndex}. It's counterpart for
 * historical segments is {@link io.druid.segment.QueryableIndexColumnSelectorFactory}.
 */
class IncrementalIndexColumnSelectorFactory implements ColumnSelectorFactory
{
  private final IncrementalIndex<?> index;
  private final VirtualColumns virtualColumns;
  private final boolean descending;
  private final TimeAndDimsHolder timeAndDimsHolder;

  IncrementalIndexColumnSelectorFactory(
      IncrementalIndex<?> index,
      VirtualColumns virtualColumns,
      boolean descending,
      TimeAndDimsHolder timeAndDimsHolder
  )
  {
    this.index = index;
    this.virtualColumns = virtualColumns;
    this.descending = descending;
    this.timeAndDimsHolder = timeAndDimsHolder;
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

    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return new SingleScanTimeDimSelector(
          makeLongColumnSelector(dimension),
          extractionFn,
          descending
      );
    }

    final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimensionSpec.getDimension());
    if (dimensionDesc == null) {
      // not a dimension, column may be a metric
      ColumnCapabilities capabilities = getColumnCapabilities(dimension);
      if (capabilities == null) {
        return DimensionSelectorUtils.constantSelector(null, extractionFn);
      }
      if (capabilities.getType() == ValueType.LONG) {
        return new LongWrappingDimensionSelector(makeLongColumnSelector(dimension), extractionFn);
      }
      if (capabilities.getType() == ValueType.FLOAT) {
        return new FloatWrappingDimensionSelector(makeFloatColumnSelector(dimension), extractionFn);
      }
      if (capabilities.getType() == ValueType.DOUBLE) {
        return new DoubleWrappingDimensionSelector(makeDoubleColumnSelector(dimension), extractionFn);
      }

      // if we can't wrap the base column, just return a column of all nulls
      return DimensionSelectorUtils.constantSelector(null, extractionFn);
    } else {
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      return indexer.makeDimensionSelector(dimensionSpec, timeAndDimsHolder, dimensionDesc);
    }
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeFloatColumnSelector(columnName, this);
    }

    final Integer dimIndex = index.getDimensionIndex(columnName);
    if (dimIndex != null) {
      final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      return indexer.makeFloatColumnSelector(timeAndDimsHolder, dimensionDesc);
    }

    final Integer metricIndexInt = index.getMetricIndex(columnName);
    if (metricIndexInt == null) {
      return ZeroFloatColumnSelector.instance();
    }

    final int metricIndex = metricIndexInt;
    return new FloatColumnSelector()
    {
      @Override
      public float getFloat()
      {
        return index.getMetricFloatValue(timeAndDimsHolder.getValue(), metricIndex);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("index", index);
      }
    };
  }

  @Override
  public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeDoubleColumnSelector(columnName, this);
    }

    final Integer dimIndex = index.getDimensionIndex(columnName);
    if (dimIndex != null) {
      final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      return indexer.makeDoubleColumnSelector(timeAndDimsHolder, dimensionDesc);
    }

    final Integer metricIndexInt = index.getMetricIndex(columnName);
    if (metricIndexInt == null) {
      return ZeroDoubleColumnSelector.instance();
    }

    final int metricIndex = metricIndexInt;
    return new DoubleColumnSelector()
    {
      @Override
      public double getDouble()
      {
        return index.getMetricDoubleValue(timeAndDimsHolder.getValue(), metricIndex);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("index", index);
      }
    };
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeLongColumnSelector(columnName, this);
    }

    if (columnName.equals(Column.TIME_COLUMN_NAME)) {
      class TimeLongColumnSelector implements LongColumnSelector
      {
        @Override
        public long getLong()
        {
          return timeAndDimsHolder.getKey().getTimestamp();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // nothing to inspect
        }
      }
      return new TimeLongColumnSelector();
    }

    final Integer dimIndex = index.getDimensionIndex(columnName);
    if (dimIndex != null) {
      final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      return indexer.makeLongColumnSelector(timeAndDimsHolder, dimensionDesc);
    }

    final Integer metricIndexInt = index.getMetricIndex(columnName);
    if (metricIndexInt == null) {
      return ZeroLongColumnSelector.instance();
    }

    final int metricIndex = metricIndexInt;

    return new LongColumnSelector()
    {
      @Override
      public long getLong()
      {
        return index.getMetricLongValue(timeAndDimsHolder.getValue(), metricIndex);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("index", index);
      }
    };
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(String column)
  {
    if (virtualColumns.exists(column)) {
      return virtualColumns.makeObjectColumnSelector(column, this);
    }

    if (column.equals(Column.TIME_COLUMN_NAME)) {
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
          return timeAndDimsHolder.getKey().getTimestamp();
        }
      };
    }

    final Integer metricIndexInt = index.getMetricIndex(column);
    if (metricIndexInt != null) {
      final int metricIndex = metricIndexInt;
      final Class classOfObject = index.getMetricClass(column);
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return classOfObject;
        }

        @Override
        public Object getObject()
        {
          return index.getMetricObjectValue(
              timeAndDimsHolder.getValue(),
              metricIndex
          );
        }
      };
    }

    IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(column);

    if (dimensionDesc == null) {
      return null;
    } else {

      final int dimensionIndex = dimensionDesc.getIndex();
      final DimensionIndexer indexer = dimensionDesc.getIndexer();

      return new ObjectColumnSelector<Object>()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object getObject()
        {
          IncrementalIndex.TimeAndDims key = timeAndDimsHolder.getKey();
          if (key == null) {
            return null;
          }

          Object[] dims = key.getDims();
          if (dimensionIndex >= dims.length) {
            return null;
          }

          return indexer.convertUnsortedEncodedKeyComponentToActualArrayOrList(
              dims[dimensionIndex], DimensionIndexer.ARRAY
          );
        }
      };
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.getColumnCapabilities(columnName);
    }

    return index.getCapabilities(columnName);
  }
}
