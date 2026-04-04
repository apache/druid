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

package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.groupby.DeferExpressionDimensions;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.SelectableColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Base class for virtual columns that are specialized implementations of expressions. All methods delegate
 * to {@link ExpressionVirtualColumn}. Subclasses may override individual methods with more optimized versions.
 */
public abstract class SpecializedExpressionVirtualColumn implements VirtualColumn
{
  protected final ExpressionVirtualColumn delegate;

  public SpecializedExpressionVirtualColumn(final ExpressionVirtualColumn delegate)
  {
    this.delegate = delegate;
  }

  @Override
  @JsonProperty("name")
  public String getOutputName()
  {
    return delegate.getOutputName();
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return delegate.canVectorize(inspector);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      final DimensionSpec dimensionSpec,
      final ColumnSelectorFactory columnSelectorFactory,
      @Nullable final ColumnSelector columnSelector,
      @Nullable final ReadableOffset offset
  )
  {
    return delegate.makeDimensionSelector(dimensionSpec, columnSelectorFactory, columnSelector, offset);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory columnSelectorFactory,
      @Nullable ColumnSelector columnSelector,
      @Nullable ReadableOffset offset
  )
  {
    return delegate.makeColumnValueSelector(columnName, columnSelectorFactory, columnSelector, offset);
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      final DimensionSpec dimensionSpec,
      final VectorColumnSelectorFactory factory,
      final ColumnSelector columnSelector,
      final ReadableVectorOffset offset
  )
  {
    return delegate.makeSingleValueVectorDimensionSelector(dimensionSpec, factory, columnSelector, offset);
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      final DimensionSpec dimensionSpec,
      final VectorColumnSelectorFactory factory,
      final ColumnSelector columnSelector,
      final ReadableVectorOffset offset
  )
  {
    return delegate.makeMultiValueVectorDimensionSelector(dimensionSpec, factory, columnSelector, offset);
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(
      final String columnName,
      final VectorColumnSelectorFactory factory,
      final ColumnSelector columnSelector,
      final ReadableVectorOffset offset
  )
  {
    return delegate.makeVectorValueSelector(columnName, factory, columnSelector, offset);
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(
      final String columnName,
      final VectorColumnSelectorFactory factory,
      final ColumnSelector columnSelector,
      final ReadableVectorOffset offset
  )
  {
    return delegate.makeVectorObjectSelector(columnName, factory, columnSelector, offset);
  }

  @Nullable
  @Override
  public GroupByVectorColumnSelector makeGroupByVectorColumnSelector(
      String columnName,
      VectorColumnSelectorFactory factory,
      DeferExpressionDimensions deferExpressionDimensions
  )
  {
    return delegate.makeGroupByVectorColumnSelector(columnName, factory, deferExpressionDimensions);
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector columnIndexSelector
  )
  {
    return delegate.getIndexSupplier(columnName, columnIndexSelector);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return delegate.capabilities(columnName);
  }

  @Nullable
  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    return delegate.capabilities(inspector, columnName);
  }

  @Override
  public List<String> requiredColumns()
  {
    return delegate.requiredColumns();
  }

  @Override
  public boolean usesDotNotation()
  {
    return delegate.usesDotNotation();
  }

  @Override
  public byte[] getCacheKey()
  {
    return delegate.getCacheKey();
  }

  @Override
  @Nullable
  public EquivalenceKey getEquivalanceKey()
  {
    return delegate.getEquivalanceKey();
  }

  @Override
  public SelectableColumn toSelectableColumn(ColumnIndexSelector columnSelector)
  {
    return delegate.toSelectableColumn(columnSelector);
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SpecializedExpressionVirtualColumn that = (SpecializedExpressionVirtualColumn) o;
    return Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(delegate);
  }
}
