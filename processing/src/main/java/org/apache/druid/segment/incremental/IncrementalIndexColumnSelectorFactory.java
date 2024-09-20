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

package org.apache.druid.segment.incremental;

import org.apache.druid.query.Order;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.SingleScanTimeDimensionSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nullable;

/**
 * The basic implementation of {@link ColumnSelectorFactory} over an {@link IncrementalIndex}. It's counterpart for
 * historical segments is {@link org.apache.druid.segment.QueryableIndexColumnSelectorFactory}.
 */
class IncrementalIndexColumnSelectorFactory implements ColumnSelectorFactory, RowIdSupplier
{
  private final ColumnInspector snapshotColumnInspector;
  private final VirtualColumns virtualColumns;
  private final Order timeOrder;
  private final IncrementalIndexRowHolder rowHolder;
  private final IncrementalIndexRowSelector rowSelector;

  IncrementalIndexColumnSelectorFactory(
      IncrementalIndexRowSelector rowSelector,
      VirtualColumns virtualColumns,
      Order timeOrder,
      IncrementalIndexRowHolder rowHolder
  )
  {
    this.virtualColumns = virtualColumns;
    this.timeOrder = timeOrder;
    this.rowHolder = rowHolder;
    this.rowSelector = rowSelector;
    this.snapshotColumnInspector = new ColumnInspector()
    {
      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return IncrementalIndexCursorFactory.snapshotColumnCapabilities(rowSelector, column);
      }
    };
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

    if (dimension.equals(ColumnHolder.TIME_COLUMN_NAME) && timeOrder != Order.NONE) {
      return new SingleScanTimeDimensionSelector(
          makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
          extractionFn,
          timeOrder
      );
    }

    final IncrementalIndex.DimensionDesc dimensionDesc = rowSelector.getDimension(dimensionSpec.getDimension());
    if (dimensionDesc == null) {
      // not a dimension, column may be a metric
      ColumnCapabilities capabilities = getColumnCapabilities(dimension);
      if (capabilities == null) {
        return DimensionSelector.constant(null, extractionFn);
      }
      if (capabilities.isNumeric()) {
        return ValueTypes.makeNumericWrappingDimensionSelector(
            capabilities.getType(),
            makeColumnValueSelector(dimension),
            extractionFn
        );
      }

      // if we can't wrap the base column, just return a column of all nulls
      return DimensionSelector.constant(null, extractionFn);
    } else {
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      return indexer.makeDimensionSelector(dimensionSpec, rowHolder, dimensionDesc);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeColumnValueSelector(columnName, this);
    }
    if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
      return rowHolder;
    }

    final IncrementalIndex.DimensionDesc dimensionDesc = rowSelector.getDimension(columnName);
    if (dimensionDesc != null) {
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      return indexer.makeColumnValueSelector(rowHolder, dimensionDesc);
    }

    return IncrementalIndex.makeMetricColumnValueSelector(rowSelector, rowHolder, columnName);
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    // Use snapshotColumnInspector instead of index.getCapabilities (see note in IncrementalIndexStorageAdapater)
    return virtualColumns.getColumnCapabilitiesWithFallback(snapshotColumnInspector, columnName);
  }

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return this;
  }

  @Override
  public long getRowId()
  {
    return rowHolder.get().getRowIndex();
  }
}
