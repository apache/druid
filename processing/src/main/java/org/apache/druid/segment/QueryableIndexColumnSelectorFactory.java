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

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueTypes;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.nested.NestedDataComplexColumn;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * The basic implementation of {@link ColumnSelectorFactory} over a historical segment (i. e. {@link QueryableIndex}).
 * It's counterpart for incremental index is {@link
 * org.apache.druid.segment.incremental.IncrementalIndexColumnSelectorFactory}.
 */
public class QueryableIndexColumnSelectorFactory implements ColumnSelectorFactory, RowIdSupplier
{
  private final VirtualColumns virtualColumns;
  private final boolean descending;
  protected final ReadableOffset offset;

  // Share Column objects, since they cache decompressed buffers internally, and we can avoid recomputation if the
  // same column is used by more than one part of a query.
  private final ColumnCache columnCache;

  // Share selectors too, for the same reason that we cache columns (they may cache things internally).
  private final Map<DimensionSpec, DimensionSelector> dimensionSelectorCache;
  private final Map<String, ColumnValueSelector> valueSelectorCache;

  public QueryableIndexColumnSelectorFactory(
      VirtualColumns virtualColumns,
      boolean descending,
      ReadableOffset offset,
      ColumnCache columnCache
  )
  {
    this.virtualColumns = virtualColumns;
    this.descending = descending;
    this.offset = offset;
    this.columnCache = columnCache;
    this.dimensionSelectorCache = new HashMap<>();
    this.valueSelectorCache = new HashMap<>();
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    Function<DimensionSpec, DimensionSelector> mappingFunction = spec -> {
      if (virtualColumns.exists(spec.getDimension())) {
        DimensionSelector dimensionSelector = virtualColumns.makeDimensionSelector(dimensionSpec, columnCache, offset);
        if (dimensionSelector == null) {
          return virtualColumns.makeDimensionSelector(dimensionSpec, this);
        } else {
          return dimensionSelector;
        }
      }

      return spec.decorate(makeDimensionSelectorUndecorated(spec));
    };

    // We cannot use dimensionSelectorCache.computeIfAbsent() here since the function being
    // applied may modify the dimensionSelectorCache itself through virtual column references,
    // triggering a ConcurrentModificationException in JDK 9 and above.
    DimensionSelector dimensionSelector = dimensionSelectorCache.get(dimensionSpec);
    if (dimensionSelector == null) {
      dimensionSelector = mappingFunction.apply(dimensionSpec);
      dimensionSelectorCache.put(dimensionSpec, dimensionSelector);
    }

    return dimensionSelector;
  }

  private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
  {
    final String dimension = dimensionSpec.getDimension();
    final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

    final ColumnHolder columnHolder = columnCache.getColumnHolder(dimension);
    if (columnHolder == null) {
      return DimensionSelector.constant(null, extractionFn);
    }

    if (dimension.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      return new SingleScanTimeDimensionSelector(makeColumnValueSelector(dimension), extractionFn, descending);
    }

    ColumnCapabilities capabilities = columnHolder.getCapabilities();
    if (columnHolder.getCapabilities().isNumeric()) {
      return ValueTypes.makeNumericWrappingDimensionSelector(
          capabilities.getType(),
          makeColumnValueSelector(dimension),
          extractionFn
      );
    }

    final BaseColumn column = columnCache.getColumn(dimension);

    if (column instanceof DictionaryEncodedColumn) {
      return ((DictionaryEncodedColumn<?>) column).makeDimensionSelector(offset, extractionFn);
    } else if (column instanceof NestedDataComplexColumn) {
      return ((NestedDataComplexColumn) column).makeDimensionSelector(Collections.emptyList(), offset, extractionFn);
    } else {
      return DimensionSelector.constant(null, extractionFn);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    // We cannot use valueSelectorCache.computeIfAbsent() here since the function being
    // applied may modify the valueSelectorCache itself through virtual column references,
    // triggering a ConcurrentModificationException in JDK 9 and above.
    ColumnValueSelector<?> columnValueSelector = valueSelectorCache.get(columnName);
    if (columnValueSelector == null) {
      if (virtualColumns.exists(columnName)) {
        ColumnValueSelector<?> selector = virtualColumns.makeColumnValueSelector(columnName, columnCache, offset);
        if (selector == null) {
          columnValueSelector = virtualColumns.makeColumnValueSelector(columnName, this);
        } else {
          columnValueSelector = selector;
        }
      } else {
        BaseColumn column = columnCache.getColumn(columnName);

        if (column != null) {
          columnValueSelector = column.makeColumnValueSelector(offset);
        } else {
          columnValueSelector = NilColumnValueSelector.instance();
        }
      }
      valueSelectorCache.put(columnName, columnValueSelector);
    }

    return columnValueSelector;
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
    return offset.getOffset();
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return virtualColumns.getColumnCapabilitiesWithFallback(columnCache, columnName);
  }
}
