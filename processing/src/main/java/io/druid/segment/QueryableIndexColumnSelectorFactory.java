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

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.Closer;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.BaseColumn;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
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

    final Column columnDesc = index.getColumn(dimension);
    if (columnDesc == null) {
      return DimensionSelectorUtils.constantSelector(null, extractionFn);
    }

    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return new SingleScanTimeDimSelector(makeColumnValueSelector(dimension), extractionFn, descending);
    }

    ValueType type = columnDesc.getCapabilities().getType();
    if (type.isNumeric()) {
      return type.makeNumericWrappingDimensionSelector(makeColumnValueSelector(dimension), extractionFn);
    }

    @SuppressWarnings("unchecked")
    DictionaryEncodedColumn<String> column = (DictionaryEncodedColumn<String>)
        columnCache.computeIfAbsent(dimension, d -> closer.register(columnDesc.getDictionaryEncoding()));

    if (column == null) {
      return DimensionSelectorUtils.constantSelector(null, extractionFn);
    } else {
      return column.makeDimensionSelector(offset, extractionFn);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeColumnValueSelector(columnName, this);
    }

    BaseColumn column = columnCache.get(columnName);

    if (column == null) {
      Column holder = index.getColumn(columnName);

      if (holder != null) {
        final ColumnCapabilities capabilities = holder.getCapabilities();

        if (capabilities.isDictionaryEncoded()) {
          column = holder.getDictionaryEncoding();
        } else if (capabilities.getType() == ValueType.COMPLEX) {
          column = holder.getComplexColumn();
        } else if (capabilities.getType().isNumeric()) {
          column = holder.getGenericColumn();
        } else {
          throw new ISE("Unknown column type: %s", capabilities.getType());
        }
        closer.register(column);
        columnCache.put(columnName, column);
      } else {
        return NilColumnValueSelector.instance();
      }
    }

    return column.makeColumnValueSelector(offset);
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
