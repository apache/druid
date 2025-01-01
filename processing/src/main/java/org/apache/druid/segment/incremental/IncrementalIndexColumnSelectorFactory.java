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

import org.apache.druid.error.DruidException;
import org.apache.druid.query.Order;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.SingleScanTimeDimensionSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

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
      IncrementalIndexRowHolder rowHolder,
      CursorBuildSpec cursorBuildSpec,
      Order timeOrder
  )
  {
    this.rowSelector = rowSelector;
    this.virtualColumns = cursorBuildSpec.getVirtualColumns();
    this.timeOrder = timeOrder;
    this.rowHolder = rowHolder;

    final Map<String, ColumnCapabilities> capabilitiesMap = new HashMap<>();
    if (cursorBuildSpec.getPhysicalColumns() == null) {
      // add everything
      for (String column : rowSelector.getDimensionNames(true)) {
        capabilitiesMap.put(column, IncrementalIndexCursorFactory.snapshotColumnCapabilities(rowSelector, column));
      }
      for (String column : rowSelector.getMetricNames()) {
        capabilitiesMap.put(column, IncrementalIndexCursorFactory.snapshotColumnCapabilities(rowSelector, column));
      }
      for (String column : cursorBuildSpec.getVirtualColumns().getColumnNames()) {
        capabilitiesMap.put(column, IncrementalIndexCursorFactory.snapshotColumnCapabilities(rowSelector, column));
      }
    } else {
      // just add required columns
      for (String column : cursorBuildSpec.getPhysicalColumns()) {
        capabilitiesMap.put(column, IncrementalIndexCursorFactory.snapshotColumnCapabilities(rowSelector, column));
      }
      // and virtual columns
      for (String column : cursorBuildSpec.getVirtualColumns().getColumnNames()) {
        capabilitiesMap.put(column, IncrementalIndexCursorFactory.snapshotColumnCapabilities(rowSelector, column));
      }
    }
    this.snapshotColumnInspector = new ColumnInspector()
    {
      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        final ColumnCapabilities capabilities = capabilitiesMap.get(column);

        DruidException.conditionalDefensive(
            cursorBuildSpec.getPhysicalColumns() == null || capabilities != null || capabilitiesMap.containsKey(column),
            "Asked for physical column capabilities for column[%s] which wasn't specified as required by the query (specified columns[%s])",
            column,
            cursorBuildSpec.getPhysicalColumns()
        );
        return capabilities;
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

    if (isTimeColumn(dimension) && timeOrder != Order.NONE) {
      return new SingleScanTimeDimensionSelector(makeColumnValueSelector(dimension), extractionFn, timeOrder);
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
    if (isTimeColumn(columnName)) {
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
    // Use snapshotColumnInspector instead of 'live' rowSelector.getCapabilities because the snapshot is frozen in time
    // at approximately when this selector factory was created (e.g. taken just after max row id)
    if (isTimeColumn(columnName)) {
      return virtualColumns.getColumnCapabilitiesWithFallback(snapshotColumnInspector, ColumnHolder.TIME_COLUMN_NAME);
    }
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

  private boolean isTimeColumn(String columnName)
  {
    return ColumnHolder.TIME_COLUMN_NAME.equals(columnName);
  }
}
