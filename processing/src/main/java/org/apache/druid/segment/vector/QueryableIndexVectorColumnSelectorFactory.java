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

package org.apache.druid.segment.vector;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.nested.NestedDataComplexColumn;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class QueryableIndexVectorColumnSelectorFactory implements VectorColumnSelectorFactory
{
  private final VirtualColumns virtualColumns;
  private final QueryableIndex index;
  private final ReadableVectorOffset offset;
  private final ColumnCache columnCache;

  // Shared selectors are useful, since they cache vectors internally, and we can avoid recomputation if the same
  // selector is used by more than one part of a query.
  private final Map<DimensionSpec, SingleValueDimensionVectorSelector> singleValueDimensionSelectorCache;
  private final Map<DimensionSpec, MultiValueDimensionVectorSelector> multiValueDimensionSelectorCache;
  private final Map<String, VectorValueSelector> valueSelectorCache;
  private final Map<String, VectorObjectSelector> objectSelectorCache;

  public QueryableIndexVectorColumnSelectorFactory(
      final QueryableIndex index,
      final ReadableVectorOffset offset,
      final ColumnCache columnCache,
      final VirtualColumns virtualColumns
  )
  {
    this.index = index;
    this.offset = offset;
    this.virtualColumns = virtualColumns;
    this.columnCache = columnCache;
    this.singleValueDimensionSelectorCache = new HashMap<>();
    this.multiValueDimensionSelectorCache = new HashMap<>();
    this.valueSelectorCache = new HashMap<>();
    this.objectSelectorCache = new HashMap<>();
  }

  @Override
  public ReadableVectorInspector getReadableVectorInspector()
  {
    return offset;
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(final DimensionSpec dimensionSpec)
  {
    if (!dimensionSpec.canVectorize()) {
      throw new ISE("DimensionSpec[%s] cannot be vectorized", dimensionSpec);
    }
    Function<DimensionSpec, MultiValueDimensionVectorSelector> mappingFunction = spec -> {
      if (virtualColumns.exists(spec.getDimension())) {
        MultiValueDimensionVectorSelector dimensionSelector = virtualColumns.makeMultiValueDimensionVectorSelector(
            dimensionSpec,
            columnCache,
            offset
        );
        if (dimensionSelector == null) {
          return virtualColumns.makeMultiValueDimensionVectorSelector(dimensionSpec, this);
        } else {
          return dimensionSelector;
        }
      }

      final ColumnHolder holder = index.getColumnHolder(spec.getDimension());
      if (holder == null
          || holder.getCapabilities().isDictionaryEncoded().isFalse()
          || !holder.getCapabilities().is(ValueType.STRING)
          || holder.getCapabilities().hasMultipleValues().isFalse()) {
        throw new ISE(
            "Column[%s] is not a multi-value string column, do not ask for a multi-value selector",
            spec.getDimension()
        );
      }

      @SuppressWarnings("unchecked")
      final DictionaryEncodedColumn<String> dictionaryEncodedColumn = (DictionaryEncodedColumn<String>)
          columnCache.getColumn(spec.getDimension());

      // dictionaryEncodedColumn is not null because of holder null check above
      assert dictionaryEncodedColumn != null;
      final MultiValueDimensionVectorSelector selector = dictionaryEncodedColumn.makeMultiValueDimensionVectorSelector(
          offset
      );

      return spec.decorate(selector);
    };

    // We cannot use computeIfAbsent() here since the function being applied may modify the cache itself through
    // virtual column references, triggering a ConcurrentModificationException in JDK 9 and above.
    MultiValueDimensionVectorSelector selector = multiValueDimensionSelectorCache.get(dimensionSpec);
    if (selector == null) {
      selector = mappingFunction.apply(dimensionSpec);
      multiValueDimensionSelectorCache.put(dimensionSpec, selector);
    }

    return selector;
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(final DimensionSpec dimensionSpec)
  {
    if (!dimensionSpec.canVectorize()) {
      throw new ISE("DimensionSpec[%s] cannot be vectorized", dimensionSpec);
    }

    Function<DimensionSpec, SingleValueDimensionVectorSelector> mappingFunction = spec -> {
      if (virtualColumns.exists(spec.getDimension())) {
        SingleValueDimensionVectorSelector dimensionSelector = virtualColumns.makeSingleValueDimensionVectorSelector(
            dimensionSpec,
            columnCache,
            offset
        );
        if (dimensionSelector == null) {
          return virtualColumns.makeSingleValueDimensionVectorSelector(dimensionSpec, this);
        } else {
          return dimensionSelector;
        }
      }

      final ColumnHolder holder = index.getColumnHolder(spec.getDimension());
      if (holder == null
          || !holder.getCapabilities().isDictionaryEncoded().isTrue()
          || !holder.getCapabilities().is(ValueType.STRING)) {
        // Asking for a single-value dimension selector on a non-string column gets you a bunch of nulls.
        return NilVectorSelector.create(offset);
      }

      if (holder.getCapabilities().hasMultipleValues().isMaybeTrue()) {
        // Asking for a single-value dimension selector on a multi-value column gets you an error.
        throw new ISE("Column[%s] is multi-value, do not ask for a single-value selector", spec.getDimension());
      }

      final BaseColumn column = columnCache.getColumn(spec.getDimension());
      final SingleValueDimensionVectorSelector selector;
      if (column instanceof DictionaryEncodedColumn) {
        selector = ((DictionaryEncodedColumn<?>) column).makeSingleValueDimensionVectorSelector(offset);
      } else if (column instanceof NestedDataComplexColumn) {
        selector = ((NestedDataComplexColumn) column).makeSingleValueDimensionVectorSelector(Collections.emptyList(), offset);
      } else {
        selector = NilVectorSelector.create(offset);
      }

      return spec.decorate(selector);
    };

    // We cannot use computeIfAbsent() here since the function being applied may modify the cache itself through
    // virtual column references, triggering a ConcurrentModificationException in JDK 9 and above.
    SingleValueDimensionVectorSelector selector = singleValueDimensionSelectorCache.get(dimensionSpec);
    if (selector == null) {
      selector = mappingFunction.apply(dimensionSpec);
      singleValueDimensionSelectorCache.put(dimensionSpec, selector);
    }

    return selector;
  }

  @Override
  public VectorValueSelector makeValueSelector(final String columnName)
  {
    Function<String, VectorValueSelector> mappingFunction = name -> {
      if (virtualColumns.exists(columnName)) {
        VectorValueSelector selector = virtualColumns.makeVectorValueSelector(columnName, columnCache, offset);
        if (selector == null) {
          return virtualColumns.makeVectorValueSelector(columnName, this);
        } else {
          return selector;
        }
      }
      final BaseColumn column = columnCache.getColumn(name);
      if (column == null) {
        return NilVectorSelector.create(offset);
      } else {
        return column.makeVectorValueSelector(offset);
      }
    };
    // We cannot use computeIfAbsent() here since the function being applied may modify the cache itself through
    // virtual column references, triggering a ConcurrentModificationException in JDK 9 and above.
    VectorValueSelector columnValueSelector = valueSelectorCache.get(columnName);
    if (columnValueSelector == null) {
      columnValueSelector = mappingFunction.apply(columnName);
      valueSelectorCache.put(columnName, columnValueSelector);
    }

    return columnValueSelector;
  }

  @Override
  public VectorObjectSelector makeObjectSelector(final String columnName)
  {
    Function<String, VectorObjectSelector> mappingFunction = name -> {
      if (virtualColumns.exists(columnName)) {
        VectorObjectSelector selector = virtualColumns.makeVectorObjectSelector(columnName, columnCache, offset);
        if (selector == null) {
          return virtualColumns.makeVectorObjectSelector(columnName, this);
        } else {
          return selector;
        }
      }
      final BaseColumn column = columnCache.getColumn(name);
      if (column == null) {
        return NilVectorSelector.create(offset);
      } else {
        return column.makeVectorObjectSelector(offset);
      }
    };
    // We cannot use computeIfAbsent() here since the function being applied may modify the cache itself through
    // virtual column references, triggering a ConcurrentModificationException in JDK 9 and above.
    VectorObjectSelector columnValueSelector = objectSelectorCache.get(columnName);
    if (columnValueSelector == null) {
      columnValueSelector = mappingFunction.apply(columnName);
      objectSelectorCache.put(columnName, columnValueSelector);
    }

    return columnValueSelector;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String columnName)
  {
    return virtualColumns.getColumnCapabilitiesWithFallback(columnCache, columnName);
  }
}
