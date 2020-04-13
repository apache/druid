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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class QueryableIndexVectorColumnSelectorFactory implements VectorColumnSelectorFactory
{
  private final QueryableIndex index;
  private final ReadableVectorOffset offset;
  private final Closer closer;
  private final Map<String, BaseColumn> columnCache;

  // Shared selectors are useful, since they cache vectors internally, and we can avoid recomputation if the same
  // selector is used by more than one part of a query.
  private final Map<DimensionSpec, SingleValueDimensionVectorSelector> singleValueDimensionSelectorCache;
  private final Map<DimensionSpec, MultiValueDimensionVectorSelector> multiValueDimensionSelectorCache;
  private final Map<String, VectorValueSelector> valueSelectorCache;
  private final Map<String, VectorObjectSelector> objectSelectorCache;

  public QueryableIndexVectorColumnSelectorFactory(
      final QueryableIndex index,
      final ReadableVectorOffset offset,
      final Closer closer,
      final Map<String, BaseColumn> columnCache
  )
  {
    this.index = index;
    this.offset = offset;
    this.closer = closer;
    this.columnCache = columnCache;
    this.singleValueDimensionSelectorCache = new HashMap<>();
    this.multiValueDimensionSelectorCache = new HashMap<>();
    this.valueSelectorCache = new HashMap<>();
    this.objectSelectorCache = new HashMap<>();
  }

  @Override
  public int getMaxVectorSize()
  {
    return offset.getMaxVectorSize();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(final DimensionSpec dimensionSpec)
  {
    if (!dimensionSpec.canVectorize()) {
      throw new ISE("DimensionSpec[%s] cannot be vectorized", dimensionSpec);
    }

    return multiValueDimensionSelectorCache.computeIfAbsent(
        dimensionSpec,
        spec -> {
          final ColumnHolder holder = index.getColumnHolder(spec.getDimension());
          if (holder == null
              || !holder.getCapabilities().isDictionaryEncoded()
              || holder.getCapabilities().getType() != ValueType.STRING
              || !holder.getCapabilities().hasMultipleValues()) {
            throw new ISE(
                "Column[%s] is not a multi-value string column, do not ask for a multi-value selector",
                spec.getDimension()
            );
          }

          @SuppressWarnings("unchecked")
          final DictionaryEncodedColumn<String> dictionaryEncodedColumn = (DictionaryEncodedColumn<String>)
              getCachedColumn(spec.getDimension());

          final MultiValueDimensionVectorSelector selector = dictionaryEncodedColumn.makeMultiValueDimensionVectorSelector(
              offset
          );

          return spec.decorate(selector);
        }
    );
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(final DimensionSpec dimensionSpec)
  {
    if (!dimensionSpec.canVectorize()) {
      throw new ISE("DimensionSpec[%s] cannot be vectorized", dimensionSpec);
    }

    return singleValueDimensionSelectorCache.computeIfAbsent(
        dimensionSpec,
        spec -> {
          final ColumnHolder holder = index.getColumnHolder(spec.getDimension());
          if (holder == null
              || !holder.getCapabilities().isDictionaryEncoded()
              || holder.getCapabilities().getType() != ValueType.STRING) {
            // Asking for a single-value dimension selector on a non-string column gets you a bunch of nulls.
            return NilVectorSelector.create(offset);
          }

          if (holder.getCapabilities().hasMultipleValues()) {
            // Asking for a single-value dimension selector on a multi-value column gets you an error.
            throw new ISE("Column[%s] is multi-value, do not ask for a single-value selector", spec.getDimension());
          }

          @SuppressWarnings("unchecked")
          final DictionaryEncodedColumn<String> dictionaryEncodedColumn = (DictionaryEncodedColumn<String>)
              getCachedColumn(spec.getDimension());

          final SingleValueDimensionVectorSelector selector =
              dictionaryEncodedColumn.makeSingleValueDimensionVectorSelector(offset);

          return spec.decorate(selector);
        }
    );
  }

  @Override
  public VectorValueSelector makeValueSelector(final String columnName)
  {
    return valueSelectorCache.computeIfAbsent(
        columnName,
        name -> {
          final BaseColumn column = getCachedColumn(name);
          if (column == null) {
            return NilVectorSelector.create(offset);
          } else {
            return column.makeVectorValueSelector(offset);
          }
        }
    );
  }

  @Override
  public VectorObjectSelector makeObjectSelector(final String columnName)
  {
    return objectSelectorCache.computeIfAbsent(
        columnName,
        name -> {
          final BaseColumn column = getCachedColumn(name);
          if (column == null) {
            return NilVectorSelector.create(offset);
          } else {
            return column.makeVectorObjectSelector(offset);
          }
        }
    );
  }

  @Nullable
  private BaseColumn getCachedColumn(final String columnName)
  {
    return columnCache.computeIfAbsent(columnName, name -> {
      ColumnHolder holder = index.getColumnHolder(name);
      if (holder != null) {
        return closer.register(holder.getColumn());
      } else {
        return null;
      }
    });
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String columnName)
  {
    return QueryableIndexStorageAdapter.getColumnCapabilities(index, columnName);
  }
}
