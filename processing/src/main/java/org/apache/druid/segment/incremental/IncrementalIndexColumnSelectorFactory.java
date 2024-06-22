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

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
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
  private final IncrementalIndexStorageAdapter adapter;
  private final IncrementalIndex index;
  private final VirtualColumns virtualColumns;
  private final boolean descending;
  private final IncrementalIndexRowHolder rowHolder;

  IncrementalIndexColumnSelectorFactory(
      IncrementalIndexStorageAdapter adapter,
      VirtualColumns virtualColumns,
      boolean descending,
      IncrementalIndexRowHolder rowHolder
  )
  {
    this.adapter = adapter;
    this.index = adapter.index;
    this.virtualColumns = virtualColumns;
    this.descending = descending;
    this.rowHolder = rowHolder;
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

    if (dimension.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return new SingleScanTimeDimensionSelector(makeColumnValueSelector(dimension), extractionFn, descending);
    }

    final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimensionSpec.getDimension());
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

    if (columnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return rowHolder;
    }

    final Integer dimIndex = index.getDimensionIndex(columnName);
    if (dimIndex != null) {
      final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      return indexer.makeColumnValueSelector(rowHolder, dimensionDesc);
    }

    return index.makeMetricColumnValueSelector(columnName, rowHolder);
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    // Use adapter.getColumnCapabilities instead of index.getCapabilities (see note in IncrementalIndexStorageAdapater)
    return virtualColumns.getColumnCapabilitiesWithFallback(adapter, columnName);
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
