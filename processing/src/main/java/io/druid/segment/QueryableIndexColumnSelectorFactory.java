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
import io.druid.segment.column.BaseColumn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnHolder;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;
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

  private final Map<String, BaseColumn> columnCache;

  QueryableIndexColumnSelectorFactory(
      QueryableIndex index,
      VirtualColumns virtualColumns,
      boolean descending,
      Closer closer,
      ReadableOffset offset,
      Map<String, BaseColumn> columnCache
  )
  {
    this.index = index;
    this.virtualColumns = virtualColumns;
    this.descending = descending;
    this.closer = closer;
    this.offset = offset;
    this.columnCache = columnCache;
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

    final ColumnHolder columnHolder = index.getColumn(dimension);
    if (columnHolder == null) {
      return DimensionSelectorUtils.constantSelector(null, extractionFn);
    }

    if (dimension.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return new SingleScanTimeDimensionSelector(makeColumnValueSelector(dimension), extractionFn, descending);
    }

    ValueType type = columnHolder.getCapabilities().getType();
    if (type.isNumeric()) {
      return type.makeNumericWrappingDimensionSelector(makeColumnValueSelector(dimension), extractionFn);
    }

    BaseColumn column = columnCache.computeIfAbsent(dimension, d -> {
      BaseColumn col = columnHolder.getColumn();
      if (col instanceof DictionaryEncodedColumn) {
        return closer.register(col);
      } else {
        return null;
      }
    });

    if (column instanceof DictionaryEncodedColumn) {
      //noinspection unchecked
      return ((DictionaryEncodedColumn<String>) column).makeDimensionSelector(offset, extractionFn);
    } else {
      return DimensionSelectorUtils.constantSelector(null, extractionFn);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeColumnValueSelector(columnName, this);
    }

    BaseColumn column = columnCache.computeIfAbsent(columnName, name -> {
      ColumnHolder holder = index.getColumn(name);
      if (holder != null) {
        return closer.register(holder.getColumn());
      } else {
        return null;
      }
    });

    if (column != null) {
      return column.makeColumnValueSelector(offset);
    } else {
      return NilColumnValueSelector.instance();
    }
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.getColumnCapabilities(columnName);
    }

    return QueryableIndexStorageAdapter.getColumnCapabilities(index, columnName);
  }
}
